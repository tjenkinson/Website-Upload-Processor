package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegFileInfo;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegProgressMonitor;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.VideoThumbnail;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(final Connection dbConnection, java.io.File source, java.io.File workingDir, final File file) {
		FileTypeProcessReturnInfo returnVal = new FileTypeProcessReturnInfo();
		try {
			Config config = Config.getInstance();
			int exitVal;
			FfmpegFileInfo info;
			BigInteger totalSize = BigInteger.ZERO;
			
			DbHelper.updateStatus(dbConnection, file.getId(), "Checking video format.", null);
			// get source file information.
			info = FfmpegHelper.getFileInfo(source, workingDir);
			if (info == null) {
				logger.warn("Error retrieving info for file with id "+file.getId()+".");
				returnVal.msg = "Invalid video format.";
				return returnVal;
			}
			
			// check the duration is more than 0
			if (info.getDuration() == 0) {
				logger.warn("Cannot process VOD file with id "+file.getId()+" because it's duration is 0.");
				returnVal.msg = "Video duration must be more than 0.";
				return returnVal;
			}
			
			// get video height
			int sourceFileH = info.getH();
			List<Object> allFormatsConfig = config.getList("encoding.vodFormats");
			
			final ArrayList<Format> formats = new ArrayList<Format>();
			final ArrayList<Format> formatsToRender = new ArrayList<Format>();
			// set to the height that is one resolution larger than the source file
			int largerHeightToRender = -1;
			// build up array of format objects
			for (Object f : allFormatsConfig) {
				String[] a = ((String) f).split("-");
				int qualityDefinitionId = Integer.parseInt(a[0]);
				int h = Integer.parseInt(a[1]);
				h += h%2; // height (and width) must be multiple of 2 for libx codec
				int aBitrate = Integer.parseInt(a[2]);
				int vBitrate = Integer.parseInt(a[3]);
				double maxFr = Double.parseDouble(a[4]);
				double sourceFr = info.getFrameRate();
				double outputFr = sourceFr;
				// calculate the output frame rate.
				// it should be the source frame rate if it is <= maxFr
				// otherwise keep halving sourceFr until this is the case
				while (outputFr > maxFr) {
					outputFr = outputFr/2;
				}
				
				Format format = new Format(qualityDefinitionId, h, aBitrate, vBitrate, outputFr, new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+h), new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"progress_"+h));
				formats.add(format);
				if ((largerHeightToRender == -1 || format.h < largerHeightToRender) && format.h >= sourceFileH) {
					largerHeightToRender = format.h;
				}
			}
			
			// determine which resolutions to render. Should render one resolution higher than the source resolution, unless it matches a resolution exactly
			for (Format f : formats) {
				if (f.h > sourceFileH && f.h != largerHeightToRender) {
					logger.debug("Not rendering height "+f.h+" because it is more than the source file's height.");
					continue;
				}
				formatsToRender.add(f);
			}
			
			final String renderRequiredFormatsMsg = "Rendering video into required formats.";
			DbHelper.updateStatus(dbConnection, file.getId(), renderRequiredFormatsMsg, 0);
			
			// loop through different formats and render videos for ones that are applicable
			for (final Format f : formatsToRender) {
				
				// check if file is now marked for deletion
				try {
					PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=?");
					s.setInt(1, file.getId());
					ResultSet r = s.executeQuery();
					if (!r.next()) {
						logger.warn("File record could not be found when checking to see if file has now been deleted for file with id "+file.getId()+".");
					}
					if (r.getBoolean("ready_for_delete")) {
						logger.debug("VOD with id "+file.getId()+" has been marked for deletion so not processing any more.");
						s.close();
						return returnVal;
					}
					s.close();
				} catch (SQLException e) {
					throw(new RuntimeException("SQL error when trying to check if file still hasn't been deleted."));
				}
				
				logger.debug("Executing ffmpeg for height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps with frame rate "+f.fr+" fps.");
				
				final FfmpegProgressMonitor monitor = new FfmpegProgressMonitor(f.progressFile, info.getNoFrames());
				monitor.setCallback(new Runnable() {
					@Override
					public void run() {
						// called whenever the process percentage changes
						// calculate the actual percentage when taking all renders into account
						int actualPercentage = (int) Math.floor(((float) monitor.getPercentage()/formatsToRender.size()) + (formatsToRender.indexOf(f)*(100.0/formatsToRender.size())));
						DbHelper.updateStatus(dbConnection, file.getId(), renderRequiredFormatsMsg, actualPercentage);
					}
				});
				exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-timelimit", ""+config.getInt("ffmpeg.videoEncodeTimeLimit"), "-progress", ""+f.progressFile.getAbsolutePath(), "-i", source.getAbsolutePath(), "-vf", "scale=trunc(("+f.h+"*a)/2)*2:"+f.h, "-strict", "experimental", "-acodec", "aac", "-b:a", f.aBitrate+"k", "-ac", "2", "-ar", "48000", "-vcodec", "libx264", "-vprofile", "main", "-g", "48", "-b:v", f.vBitrate+"k", "-maxrate", f.vBitrate+"k", "-bufsize", f.vBitrate*2+"k", "-preset", "medium", "-crf", "16", "-vsync", "vfr", "-af", "aresample=async=1000", "-movflags", "+faststart", "-r", f.fr+"", "-f", "mp4", f.outputFile.getAbsolutePath()}, workingDir, null, null);
				monitor.destroy();
				if (exitVal == 0) {
					logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
					totalSize.add(BigInteger.valueOf(f.outputFile.length()));
					if (FileHelper.isOverQuota(totalSize)) {
						returnVal.msg = "Ran out of space.";
						return returnVal;
					}
				}
				else {
					logger.warn("ffmpeg finished but returned error code "+exitVal+".");
					// if any renders fail fail the whole thing.
					// already rendered files will be cleaned up later because the working directory is cleared
					returnVal.msg = "Error rendering video.";
					return returnVal;
				}
			}
			
			
			
			
			
			// generate the thumbnails that will be shown as the user scrubs through the item in the player.
			DbHelper.updateStatus(dbConnection, file.getId(), "Generating scrub thumbnails.", null);
			VideoThumbnail[] thumbnails = FfmpegHelper.generateThumbnails(config.getInt("encoding.vodScrubThumbnails.numberPerItem"), source, workingDir, config.getInt("encoding.vodScrubThumbnails.width"), config.getInt("encoding.vodScrubThumbnails.height"));
			if (thumbnails == null) {
				logger.warn("There was an error generating video scrub thumbnails.");
				returnVal.msg = "Error generating scrub thumbnails.";
				return returnVal;
			}
			
			for(VideoThumbnail thumbnail : thumbnails) {
				totalSize.add(BigInteger.valueOf(thumbnail.getFile().length()));
				if (FileHelper.isOverQuota(totalSize)) {
					returnVal.msg = "Ran out of space.";
					return returnVal;
				}
			}
			
			// this order is important to make sure if anything goes wrong there aren't any files left in the webapp files folder without a corresponding entry in the db		
			// create entries in Files with in_use set to 0
			// and copy files across to web app
			DbHelper.updateStatus(dbConnection, file.getId(), "Finalizing renders.", null);
			ArrayList<VideoOutputFile> videoOutputFiles = new ArrayList<VideoOutputFile>();
			try {
				for (Format f : formatsToRender) {
					
					logger.debug("Creating file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
					File newFile = generateNewFile(file, f.outputFile, FileType.VOD_VIDEO_RENDER, dbConnection);
					if (newFile == null) {
						logger.warn("Error trying to generate file object for output file.");
						return returnVal;
					}
					// add to set of files to mark in_use when processing completed
					if (!returnVal.registerNewFile(newFile)) {
						// error occurred. abort
						logger.warn("Error trying to register newly created file.");
						return returnVal;
					}
					logger.debug("File record created with id "+newFile.getId()+" for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
					
					
					// add entry to OutputFiles array which will be used to populate VideoFiles table later
					// get width and height of output
					info = FfmpegHelper.getFileInfo(f.outputFile, workingDir);
					if (info == null) {
						logger.warn("Error retrieving info for file rendered from source file with id "+file.getId()+".");
						return returnVal;
					}
					videoOutputFiles.add(new VideoOutputFile(newFile.getId(), info.getW(), info.getH(), f.qualityDefinitionId));
					
					// copy file to server
					logger.info("Moving output file with id "+newFile.getId()+" to web app...");
					if (!FileHelper.moveToWebApp(f.outputFile, newFile.getId())) {
						logger.error("Error trying to move output file with id "+newFile.getId()+" to web app.");
						return returnVal;
					}
					logger.info("Output video file with id "+newFile.getId()+" moved to web app.");
				}
			} catch (SQLException e) {
				throw(new RuntimeException("Error trying to register files in database."));
			}
			
			try {
				// create entries in video_files
				logger.debug("Creating entries in video_files table...");
				for (VideoOutputFile o : videoOutputFiles) {
					Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
					PreparedStatement s = dbConnection.prepareStatement("INSERT INTO video_files (width,height,created_at,updated_at,quality_definition_id,file_id) VALUES (?,?,?,?,?,?)");
					s.setInt(1, o.w);
					s.setInt(2, o.h);
					s.setTimestamp(3, currentTimestamp);
					s.setTimestamp(4, currentTimestamp);
					s.setInt(5, o.qualityDefinitionId);
					s.setInt(6, o.id);
					int result = s.executeUpdate();
					s.close();
					if (result != 1) {
						logger.debug("Error registering file with id "+o.id+" in video_files table.");
						return returnVal;
					}
					logger.debug("Created entry in video_files table for file with id "+o.id+".");
				}
				logger.debug("Created entries in video_files table.");
			} catch (SQLException e) {
				throw(new RuntimeException("Error trying to create entries in video_files."));
			}
	
	
			DbHelper.updateStatus(dbConnection, file.getId(), "Finalizing scrub thumbnails.", null);
			ArrayList<ThumbnailOutputFile> thumbnailOutputFiles = new ArrayList<ThumbnailOutputFile>();
			try {
				for (VideoThumbnail thumbnail : thumbnails) {
					
					logger.debug("Creating file record for thumbnail at time "+thumbnail.getTime()+" belonging to source file with id "+file.getId()+".");
					File newFile = generateNewFile(file, thumbnail.getFile(), FileType.VOD_SCRUB_THUMBNAIL, dbConnection);
					if (newFile == null) {
						logger.warn("Error trying to generate file object for output file.");
						return returnVal;
					}
					// add to set of files to mark in_use when processing completed
					if (!returnVal.registerNewFile(newFile)) {
						// error occurred. abort
						logger.warn("Error trying to register newly created file.");
						return returnVal;
					}
					logger.debug("File record created with id "+newFile.getId()+" for thumbnail at time "+thumbnail.getTime()+" belonging to source file with id "+file.getId()+".");
					
					thumbnailOutputFiles.add(new ThumbnailOutputFile(newFile.getId(), thumbnail));
					
					// copy file to server
					logger.info("Moving output file with id "+newFile.getId()+" to web app...");
					if (!FileHelper.moveToWebApp(thumbnail.getFile(), newFile.getId())) {
						logger.error("Error trying to move output file with id "+newFile.getId()+" to web app.");
						return returnVal;
					}
					
					logger.info("Output thumbnail file with id "+newFile.getId()+" moved to web app.");
				}
			} catch (SQLException e) {
				throw(new RuntimeException("Error trying to register files in database."));
			}
			
			try {
				// create entries in video_scrub_thumbnail_files
				logger.debug("Creating entries in video_scrub_thumbnail_files table...");
				for (ThumbnailOutputFile thumbnailOutputFile : thumbnailOutputFiles) {
					VideoThumbnail thumbnail = thumbnailOutputFile.thumbnail;
					Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
					PreparedStatement s = dbConnection.prepareStatement("INSERT INTO video_scrub_thumbnail_files (created_at,updated_at,time,file_id) VALUES (?,?,?,?)");
					s.setTimestamp(1, currentTimestamp);
					s.setTimestamp(2, currentTimestamp);
					s.setInt(3, thumbnail.getTime());
					s.setInt(4, thumbnailOutputFile.id);
					int result = s.executeUpdate();
					s.close();
					if (result != 1) {
						logger.debug("Error registering file with id "+thumbnailOutputFile.id+" in video_scrub_thumbnail_files table.");
						return returnVal;
					}
					logger.debug("Created entry in video_scrub_thumbnail_files table for file with id "+thumbnailOutputFile.id+".");
				}
				logger.debug("Created entries in video_scrub_thumbnail_files table.");
			} catch (SQLException e) {
				throw(new RuntimeException("Error trying to create entries in video_scrub_thumbnail_files."));
			}
		}
		catch(Exception e){
			e.printStackTrace();
			logger.error("An exception occurred whilst trying to process vod.");
			returnVal.msg = "An unexpected error occurred.";
			return returnVal;
		}
		returnVal.success = true;
		return returnVal;	
	}
	
	// returns the File object for the new file on success or null otherwise
	private File generateNewFile(File sourceFile, java.io.File file, FileType fileType, Connection dbConnection) throws SQLException {
		long size = file.length(); // size of file in bytes
		Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
		PreparedStatement s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,heartbeat,process_state) VALUES(0,?,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
		s.setTimestamp(1, currentTimestamp);
		s.setTimestamp(2, currentTimestamp);
		s.setLong(3, size);
		s.setInt(4, fileType.getObj().getId());
		s.setInt(5, sourceFile.getId());
		// so that nothing else will pick up this file and it can be registered with the heartbeat manager immediately
		s.setTimestamp(6, currentTimestamp);
		if (s.executeUpdate() != 1) {
			s.close();
			logger.warn("Error occurred when creating database entry for a file.");
			return null;
		}
		
		ResultSet generatedKeys = s.getGeneratedKeys();
		generatedKeys.next();
		int id = generatedKeys.getInt(1);
		s.close();
		return new File(id, null, size, fileType.getObj());
	}
		
	private class Format {
		
		public Format(int qualityDefinitionId, int h, int aBitrate, int vBitrate, double fr, java.io.File outputFile, java.io.File progressFile) {
			this.qualityDefinitionId = qualityDefinitionId;
			this.h = h;
			this.aBitrate = aBitrate;
			this.vBitrate = vBitrate;
			this.fr = fr;
			this.outputFile = outputFile;
			this.progressFile = progressFile;
		}
		
		public int h;
		public int aBitrate;
		public int vBitrate;
		public double fr; // the frame rate that the output file should be
		public int qualityDefinitionId;
		public java.io.File outputFile;
		public java.io.File progressFile;
	}
	
	private class VideoOutputFile {
		
		public int id;
		public int w;
		public int h;
		public int qualityDefinitionId;
		
		public VideoOutputFile(int id, int w, int h, int qualityDefinitionId) {
			this.id = id;
			this.w = w;
			this.h = h;
			this.qualityDefinitionId = qualityDefinitionId;
		}
	}
	
	private class ThumbnailOutputFile {
		
		public int id;
		public VideoThumbnail thumbnail;
		
		public ThumbnailOutputFile(int id, VideoThumbnail thumbnail) {
			this.id = id;
			this.thumbnail = thumbnail;
		}
	}
}
