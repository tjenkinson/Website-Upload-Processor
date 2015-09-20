package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegFileInfo;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegProgressMonitor;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.VideoThumbnail;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(final Connection dbConnection, java.io.File source, java.io.File workingDir, final File file, final boolean workingWithCopy) {
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
			if (info.getDuration() == 0 || info.getNoFrames() == 0) {
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
				
				Format format = new Format(qualityDefinitionId, h, aBitrate, vBitrate, outputFr, new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+h));
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
				if (isFileMarkedForDeleteion(dbConnection, file)) {
					logger.debug("VOD with id "+file.getId()+" has been marked for deletion so not processing any more.");
					return returnVal;
				}
				logger.debug("Executing ffmpeg for height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps with frame rate "+f.fr+" fps.");
				
				double noOutputFrames = Math.ceil(info.getNoFrames() * (f.fr / info.getFrameRate()));
				final FfmpegProgressMonitor monitor = new FfmpegProgressMonitor(f.getProgressFile(), noOutputFrames);
				monitor.setCallback(new Runnable() {
					@Override
					public void run() {
						// called whenever the process percentage changes
						// calculate the actual percentage when taking all renders into account
						int actualPercentage = (int) Math.floor(((float) monitor.getPercentage()/formatsToRender.size()) + (formatsToRender.indexOf(f)*(100.0/formatsToRender.size())));
						DbHelper.updateStatus(dbConnection, file.getId(), renderRequiredFormatsMsg, actualPercentage);
					}
				});
				exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-timelimit", ""+config.getInt("ffmpeg.videoEncodeTimeLimit"), "-progress", f.getProgressFile().getAbsolutePath(), "-i", source.getAbsolutePath(), "-vf", "scale=trunc(("+f.h+"*a)/2)*2:"+f.h, "-strict", "experimental", "-acodec", "aac", "-b:a", f.aBitrate+"k", "-ac", "2", "-ar", "48000", "-vcodec", "libx264", "-vprofile", "main", "-g", "48", "-b:v", f.vBitrate+"k", "-maxrate", f.vBitrate+"k", "-bufsize", f.vBitrate*2+"k", "-preset", "medium", "-crf", "16", "-vsync", "vfr", "-af", "aresample=async=1000", "-movflags", "+faststart", "-r", f.fr+"", "-f", "mp4", f.outputFile.getAbsolutePath()}, workingDir, null, null);
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
			
			int numDashHlsRenders = 0;
			for (final Format f : formatsToRender) {
				if (f.shouldCreateDashAndHls()) {
					f.creatingDashAndHlsRenders = true;
					numDashHlsRenders++;
				}
			}
			
			// create dash renders
			// loop through different formats and render videos for ones that are applicable
			int dashRenderNum = -1;
			for (final Format f : formatsToRender) {
				if (!f.creatingDashAndHlsRenders) {
					continue;
				}
				dashRenderNum++;
				DbHelper.updateStatus(dbConnection, file.getId(), "Creating DASH encodes.", ((int) Math.floor((dashRenderNum/(float) numDashHlsRenders) * 100)));
				
				// check if file is now marked for deletion
				if (isFileMarkedForDeleteion(dbConnection, file)) {
					logger.debug("VOD with id "+file.getId()+" has been marked for deletion so not processing any more.");
					return returnVal;
				}
			
				logger.debug("Executing mp4box to create DASH output for output file with height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps with frame rate "+f.fr+" fps.");	
				exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("mp4box.location"), "-dash", "5000", "-rap", "-frag-rap", "-profile", "onDemand", "-out", f.outputFile.getName()+"_dash", f.outputFile.getAbsolutePath()+"#video", f.outputFile.getAbsolutePath()+"#audio"}, workingDir, null, null);
				if (exitVal == 0) {
					logger.debug("mp4box finished successfully with error code "+exitVal+".");
					totalSize.add(BigInteger.valueOf(f.getDashAudioChannelFile().length()));
					totalSize.add(BigInteger.valueOf(f.getDashVideoChannelFile().length()));
					if (FileHelper.isOverQuota(totalSize)) {
						returnVal.msg = "Ran out of space.";
						return returnVal;
					}
				}
				else {
					logger.warn("mp4box finished but returned error code "+exitVal+".");
					// if any renders fail fail the whole thing.
					// already rendered files will be cleaned up later because the working directory is cleared
					returnVal.msg = "Error creating DASH render of video.";
					return returnVal;
				}
			}
			
			// create hls renders
			// loop through different formats and render videos for ones that are applicable
			int hlsRenderNum = -1;
			for (final Format f : formatsToRender) {
				if (!f.creatingDashAndHlsRenders) {
					continue;
				}
				hlsRenderNum++;
				final String renderHlsMessage = "Creating HLS encodes.";
				DbHelper.updateStatus(dbConnection, file.getId(), renderHlsMessage, ((int) Math.floor((hlsRenderNum/(float) numDashHlsRenders) * 100)));
				
				// check if file is now marked for deletion
				if (isFileMarkedForDeleteion(dbConnection, file)) {
					logger.debug("VOD with id "+file.getId()+" has been marked for deletion so not processing any more.");
					return returnVal;
				}
			
				logger.debug("Executing ffmpeg to create HLS output for output file with height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps with frame rate "+f.fr+" fps.");	
				double noOutputFrames = Math.ceil(info.getNoFrames() * (f.fr / info.getFrameRate()));
				final FfmpegProgressMonitor monitor = new FfmpegProgressMonitor(f.getHlsProgressFile(), noOutputFrames);
				final int numDashHlsRendersFinal = numDashHlsRenders;
				final int hlsRenderNumFinal = hlsRenderNum;
				monitor.setCallback(new Runnable() {
					@Override
					public void run() {
						// called whenever the process percentage changes
						// calculate the actual percentage when taking all renders into account
						int actualPercentage = (int) Math.floor(((float) monitor.getPercentage()/numDashHlsRendersFinal) + (hlsRenderNumFinal*(100.0/numDashHlsRendersFinal)));
						DbHelper.updateStatus(dbConnection, file.getId(), renderHlsMessage, actualPercentage);
					}
				});
				exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-timelimit", ""+config.getInt("ffmpeg.videoEncodeTimeLimit"), "-progress", f.getHlsProgressFile().getAbsolutePath(), "-i", f.outputFile.getAbsolutePath(), "-hls_allow_cache", "1", "-hls_time", "5", "-hls_list_size", "0", "-hls_segment_filename", f.getHlsSegmentFile().getAbsolutePath(), "-hls_flags", "single_file", "-f", "hls", f.getHlsPlaylistFile().getAbsolutePath()}, workingDir, null, null);
				monitor.destroy();
				if (exitVal == 0) {
					logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
					totalSize.add(BigInteger.valueOf(f.getHlsPlaylistFile().length()));
					totalSize.add(BigInteger.valueOf(f.getHlsSegmentFile().length()));
					if (FileHelper.isOverQuota(totalSize)) {
						returnVal.msg = "Ran out of space.";
						return returnVal;
					}
				}
				else {
					logger.warn("ffmpeg finished but returned error code "+exitVal+".");
					// if any renders fail fail the whole thing.
					// already rendered files will be cleaned up later because the working directory is cleared
					returnVal.msg = "Error creating HLS render of video.";
					return returnVal;
				}
			}
			
			// delete the source file if it is a copy to save space for the next step
			if (workingWithCopy) {
				if (!source.delete()) {
					logger.warn("Error when trying to delete copy of source file with id "+file.getId()+" before finalizing renders.");
				}
				else {
					logger.info("Deleted copy of source file with id "+file.getId()+" from working directory to save space as it is no longer needed.");
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
					
					// copy file to server
					if (!moveFileToWebApp(f.outputFile, newFile)) {
						return returnVal;
					}
					
					File dashMediaPresentationDescriptionFileObj = null;
					File dashAudioChannelFileObj = null;
					File dashVideoChannelFileObj = null;
					File hlsPlaylistFileObj = null;
					File hlsSegmentFileObj = null;
					
					if (f.creatingDashAndHlsRenders) {
						// register dash audio channel render and video channel renders
						logger.debug("Creating dash render file records for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						dashAudioChannelFileObj = generateNewFile(file, f.getDashAudioChannelFile(), FileType.DASH_SEGMENT, dbConnection);
						if (dashAudioChannelFileObj == null) {
							logger.warn("Error trying to generate file object for dash audio channel output file.");
							return returnVal;
						}
						dashVideoChannelFileObj = generateNewFile(file, f.getDashVideoChannelFile(), FileType.DASH_SEGMENT, dbConnection);
						if (dashVideoChannelFileObj == null) {
							logger.warn("Error trying to generate file object for dash video channel output file.");
							return returnVal;
						}
						
						if (!returnVal.registerNewFile(dashAudioChannelFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register dash audio channel file.");
							return returnVal;
						}
						if (!returnVal.registerNewFile(dashVideoChannelFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register dash video channel file.");
							return returnVal;
						}
						logger.debug("File records created with ids "+dashAudioChannelFileObj.getId()+" and "+dashVideoChannelFileObj.getId()+" for dash render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						
						// modify dash description file so that the paths to the audio and video channel files are correct
						java.io.File mediaPresentationFile = f.getDashMediaPresentationDescriptionFile(dashAudioChannelFileObj, dashVideoChannelFileObj);
						if (mediaPresentationFile == null) {
							logger.warn("Error getting dash media description presentation file.");
							return returnVal;
						}
						logger.debug("Creating dash media presentation description file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						dashMediaPresentationDescriptionFileObj = generateNewFile(file, mediaPresentationFile, FileType.DASH_MEDIA_PRESENTATION_DESCRIPTION, dbConnection);
						if (dashMediaPresentationDescriptionFileObj == null) {
							logger.warn("Error trying to generate file object for dash media presentation description file.");
							return returnVal;
						}
						if (!returnVal.registerNewFile(dashMediaPresentationDescriptionFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register dash media presentation description file object.");
							return returnVal;
						}

						// copy files to server
						if (!moveFileToWebApp(f.getDashAudioChannelFile(), dashAudioChannelFileObj)) {
							return returnVal;
						}
						if (!moveFileToWebApp(f.getDashVideoChannelFile(), dashVideoChannelFileObj)) {
							return returnVal;
						}
						if (!moveFileToWebApp(mediaPresentationFile, dashMediaPresentationDescriptionFileObj)) {
							return returnVal;
						}
						
						// register dash audio channel render and video channel renders
						logger.debug("Creating dash render file records for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						dashAudioChannelFileObj = generateNewFile(file, f.getDashAudioChannelFile(), FileType.DASH_SEGMENT, dbConnection);
						if (dashAudioChannelFileObj == null) {
							logger.warn("Error trying to generate file object for dash audio channel output file.");
							return returnVal;
						}
						dashVideoChannelFileObj = generateNewFile(file, f.getDashVideoChannelFile(), FileType.DASH_SEGMENT, dbConnection);
						if (dashVideoChannelFileObj == null) {
							logger.warn("Error trying to generate file object for dash video channel output file.");
							return returnVal;
						}
						
						if (!returnVal.registerNewFile(dashAudioChannelFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register dash audio channel file.");
							return returnVal;
						}
						if (!returnVal.registerNewFile(dashVideoChannelFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register dash video channel file.");
							return returnVal;
						}
						logger.debug("File records created with ids "+dashAudioChannelFileObj.getId()+" and "+dashVideoChannelFileObj.getId()+" for dash render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						
						logger.debug("Creating hls segment render file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						hlsSegmentFileObj = generateNewFile(file, f.getHlsSegmentFile(), FileType.HLS_SEGMENT, dbConnection);
						if (hlsSegmentFileObj == null) {
							logger.warn("Error trying to generate file object for hls segment output file.");
							return returnVal;
						}
						
						logger.debug("Creating hls playlist file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
						hlsPlaylistFileObj = generateNewFile(file, mediaPresentationFile, FileType.HLS_MEDIA_PLAYLIST, dbConnection);
						if (hlsPlaylistFileObj == null) {
							logger.warn("Error trying to generate file object for dash hls playlist file.");
							return returnVal;
						}
						if (!returnVal.registerNewFile(hlsPlaylistFileObj)) {
							// error occurred. abort
							logger.warn("Error trying to register hls playlist file object.");
							return returnVal;
						}
						
						java.io.File hlsPlaylistFile = f.generateHlsPlaylistFileWithCorrectPaths(hlsSegmentFileObj);
						if (hlsPlaylistFile == null) {
							logger.warn("Error generating hls playlist file.");
							return returnVal;
						}
						
						// copy files to server
						if (!moveFileToWebApp(hlsPlaylistFile, hlsPlaylistFileObj)) {
							return returnVal;
						}
						if (!moveFileToWebApp(f.getHlsSegmentFile(), hlsSegmentFileObj)) {
							return returnVal;
						}
					}
					videoOutputFiles.add(new VideoOutputFile(newFile, info.getW(), info.getH(), f.qualityDefinitionId, dashMediaPresentationDescriptionFileObj, dashAudioChannelFileObj, dashVideoChannelFileObj, hlsPlaylistFileObj, hlsSegmentFileObj));
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				throw(new RuntimeException("Error trying to register files in database."));
			}
			
			try {
				// create entries in video_files, video_files_dash and video_files_hls
				logger.debug("Creating entries in video_files and video_files_dash tables...");
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
					ResultSet generatedKeys = s.getGeneratedKeys();
					generatedKeys.next();
					int videoFilesRecordId = generatedKeys.getInt(1);
					s.close();
					if (result != 1) {
						logger.debug("Error registering file with id "+o.id+" in video_files table.");
						return returnVal;
					}
					logger.debug("Created entry in video_files table for file with id "+o.id+".");
					if (o.dashMediaPresentationDescriptionId != null) {
						// dash and hls renders created
						PreparedStatement s2 = dbConnection.prepareStatement("INSERT INTO video_files_dash (video_files_id,media_presentation_description_file_id,audio_channel_file_id,video_channel_file_id,created_at,updated_at) VALUES (?,?,?,?,?,?)");
						s2.setInt(1, videoFilesRecordId);
						s2.setInt(2, o.dashMediaPresentationDescriptionId);
						s2.setInt(3, o.dashAudioChannelId);
						s2.setInt(4, o.dashVideoChannelId);
						s2.setTimestamp(5, currentTimestamp);
						s2.setTimestamp(6, currentTimestamp);
						int result2 = s2.executeUpdate();
						s2.close();
						if (result2 != 1) {
							logger.debug("Error creating entry in video_files_dash table for video_files record id "+videoFilesRecordId+".");
							return returnVal;
						}
						logger.debug("Created entry in video_files_dash table for video_files record id "+videoFilesRecordId+".");
						
						PreparedStatement s3 = dbConnection.prepareStatement("INSERT INTO video_files_hls (video_files_id,playlist_file_id,segment_file_id,created_at,updated_at) VALUES (?,?,?,?,?)");
						s3.setInt(1, videoFilesRecordId);
						s3.setInt(2, o.hlsPlaylistFileId);
						s3.setInt(3, o.hlsSegmentFileId);
						s3.setTimestamp(4, currentTimestamp);
						s3.setTimestamp(5, currentTimestamp);
						int result3 = s3.executeUpdate();
						s3.close();
						if (result3 != 1) {
							logger.debug("Error creating entry in video_files_dash table for video_files record id "+videoFilesRecordId+".");
							return returnVal;
						}
						logger.debug("Created entry in video_files_dash table for video_files record id "+videoFilesRecordId+".");
					}
				}
				logger.debug("Created entries in video_files and video_files_dash tables.");
			} catch (SQLException e) {
				e.printStackTrace();
				throw(new RuntimeException("Error trying to create entries in video_files and video_files_dash."));
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
				e.printStackTrace();
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
		while(true) {
			int numAttempts = 0;
			try {
				if (s.executeUpdate() != 1) {
					s.close();
					logger.warn("Error occurred when creating database entry for a file.");
					return null;
				}
				else {
					break;
				}
			}
			catch(MySQLTransactionRollbackException e) {
				// http://stackoverflow.com/a/17748793/1048589 and http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
				if (++numAttempts >= 3) {
					// if this has failed 3 times then abort by rethrowing the exception.
					s.close();
					throw(e);
				}
				e.printStackTrace();
				logger.warn("Mysql deadlock occurred. Retrying.");
			}
		}
		
		ResultSet generatedKeys = s.getGeneratedKeys();
		generatedKeys.next();
		int id = generatedKeys.getInt(1);
		s.close();
		return new File(id, null, size, fileType.getObj());
	}
	
	private boolean moveFileToWebApp(java.io.File file, File fileObj) {
		logger.info("Moving output file with id "+fileObj.getId()+" to web app...");
		if (!FileHelper.moveToWebApp(file, fileObj.getId())) {
			logger.error("Error trying to move output file with id "+fileObj.getId()+" to web app.");
			return false;
		}
		logger.info("Output video file with id "+fileObj.getId()+" moved to web app.");
		return true;
	}
	
	private boolean isFileMarkedForDeleteion(Connection dbConnection, File file) {
		try {
			PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=?");
			s.setInt(1, file.getId());
			ResultSet r = s.executeQuery();
			if (!r.next()) {
				logger.warn("File record could not be found when checking to see if file has now been deleted for file with id "+file.getId()+".");
				s.close();
				// pretend it's been deleted
				return true;
			}
			if (r.getBoolean("ready_for_delete")) {
				logger.debug("File with id "+file.getId()+" has been marked for deletion.");
				s.close();
				return true;
			}
			s.close();
			return false;
		} catch (SQLException e) {
			throw(new RuntimeException("SQL error when trying to check if file still hasn't been deleted."));
		}
	}
		
	private class Format {
		
		public Format(int qualityDefinitionId, int h, int aBitrate, int vBitrate, double fr, java.io.File outputFile) {
			this.qualityDefinitionId = qualityDefinitionId;
			this.h = h;
			this.aBitrate = aBitrate;
			this.vBitrate = vBitrate;
			this.fr = fr;
			this.outputFile = outputFile;
		}
		
		public java.io.File getProgressFile() {
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_progress"));
		}
		
		public java.io.File getHlsProgressFile() {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_hls_progress"));
		}
		
		public java.io.File getHlsPlaylistFile() {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_hls"));
		}
		
		public java.io.File getHlsSegmentFile() {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_hls_segment"));
		}
		
		// will return a File which points to a modified version of the hls playlist file that has the correct paths to segment file
		// or null if there was an error generating the file, or a dash render should not be created
		public java.io.File generateHlsPlaylistFileWithCorrectPaths(File segmentFile) {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			java.io.File destinationPlaylistFile = new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_hls_updated.mpd"));
			java.io.File sourcePlaylistFile = getHlsPlaylistFile();
			
			if (!sourcePlaylistFile.exists()) {
				throw(new RuntimeException("Could not find source hls playlist file."));
			}
			
			if (destinationPlaylistFile.exists()) {
				if (!destinationPlaylistFile.delete()) {
					logger.error("Unable to delete existing generated hls playlist file.");
					return null;
				}
			}
			
			// scan through the source file and replace any instances of the path with the new one
			try(
				// these resources will be automatically closed
				BufferedReader br = new BufferedReader(new FileReader(sourcePlaylistFile));
				FileOutputStream fos = new FileOutputStream(destinationPlaylistFile);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			) {
			    for(String line; (line = br.readLine()) != null; ) {
			    	if (line.length() > 0) {
			    		if (line.charAt(0) == '#') {
			    			// just copy line to output as is
			    			bw.write(line);
							bw.newLine();
			    		}
			    		else {
			    			// line isn't empty and therefore can assume is the file path
			    			// write the new file path
			    			bw.write(""+segmentFile.getId());
							bw.newLine();
			    		}
			    	}
			    }
			} catch (IOException e) {
				logger.error("Error generating hls playlist file.");
				return null;
			}
			return destinationPlaylistFile;
		}
		
		public boolean shouldCreateDashAndHls() {
			if (!outputFile.exists()) {
				throw(new RuntimeException("Can not determine if a dash render should be created because the initial render file is missing."));
			}
			return outputFile.length() > Config.getInstance().getLong("encoding.minSizeRequiredForDashEncode")*1000000;
		}
		
		public java.io.File getDashAudioChannelFile() {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			// mp4box will render to this file.
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_track1_dashinit.mp4"));
		}
		
		public java.io.File getDashVideoChannelFile() {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			return new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_track2_dashinit.mp4"));
		}
		
		// will return a File which points to a modified version of the presentation description file that has the correct paths to the audio and video channel files
		// or null if there was an error generating the file, or a dash render should not be created
		public java.io.File getDashMediaPresentationDescriptionFile(File audioChannelFile, File videoChannelFile) {
			if (!creatingDashAndHlsRenders) {
				return null;
			}
			java.io.File destinationDescriptionFile = new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_dash_updated.mpd"));
			// mp4box will generate this
			java.io.File sourceDescriptionFile = new java.io.File(FileHelper.format(outputFile.getParentFile().getAbsolutePath()+"/"+outputFile.getName()+"_dash.mpd"));
			
			if (!sourceDescriptionFile.exists()) {
				throw(new RuntimeException("Could not find source media presentation description file."));
			}
			
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder;
			try {
				docBuilder = docFactory.newDocumentBuilder();
				Document doc = docBuilder.parse(sourceDescriptionFile);
				Node[] roots = extractNodesFromNodeList(doc.getChildNodes(), "MPD");
				if (roots.length != 1) {
					logger.error("Could not find \"MPD\" element in dash media presentation description file.");
					return null;
				}
				Node root = roots[0];
				Node[] periodNodes = extractNodesFromNodeList(root.getChildNodes(), "Period");
				if (periodNodes.length != 1) {
					logger.error("Could not find \"period\" element in dash media presentation description file.");
					return null;
				}
				Node periodNode = periodNodes[0];
				Node[] adaptationSetNodes = extractNodesFromNodeList(periodNode.getChildNodes(), "AdaptationSet");
				if (adaptationSetNodes.length != 2) {
					logger.error("Period in dash media presentation description file not as expected.");
					return null;
				}
				Node audioAdaptationSetNode = adaptationSetNodes[0];
				NodeList audioAdaptationSetNodeList = audioAdaptationSetNode.getChildNodes();
				// the relative url to the file will just be the id of the file
				if (!setBaseUrlInAdaptationSet(audioAdaptationSetNodeList, audioChannelFile.getId()+"")) {
					return null;
				}
				Node videoAdaptationSetNode = adaptationSetNodes[1];
				NodeList videoAdaptationSetNodeList = videoAdaptationSetNode.getChildNodes();
				// the relative url to the file will just be the id of the file
				if (!setBaseUrlInAdaptationSet(videoAdaptationSetNodeList, videoChannelFile.getId()+"")) {
					return null;
				}
				
				// write the new file
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(doc);
				if (destinationDescriptionFile.exists()) {
					if (!destinationDescriptionFile.delete()) {
						logger.error("Unable to delete existing generated dash media presentation description file.");
						return null;
					}
				}
				StreamResult result = new StreamResult(destinationDescriptionFile);
				transformer.transform(source, result);
			
			} catch (ParserConfigurationException | SAXException | IOException | TransformerException e) {
				logger.error("Error generating dash media presentation description file.");
				return null;
			}
			return destinationDescriptionFile;
		}
		
		private Node[] extractNodesFromNodeList(NodeList nodes, String name) {
			ArrayList<Node> nodeObjects = new ArrayList<Node>();
			for (int i=0; i<nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (name.equals(node.getNodeName())){
					nodeObjects.add(node);
				}
			}
			return nodeObjects.toArray(new Node[nodeObjects.size()]);
		}
		
		private boolean setBaseUrlInAdaptationSet(NodeList adaptationSetNodeList, String value) {
			Node[] representationNodes = extractNodesFromNodeList(adaptationSetNodeList, "Representation");
			if (representationNodes.length != 1) {
				logger.error("Adaptation set in dash media presentation description file not as expected.");
				return false;
			}
			Node representationNode = representationNodes[0];
			NodeList representationNodeList = representationNode.getChildNodes();
			Node[] baseUrlNodes = extractNodesFromNodeList(representationNodeList, "BaseURL");

			if (baseUrlNodes.length != 1) {
				logger.error("Could not find \"BaseURL\" element in dash media presentation description file.");
				return false;
			}
			Node baseUrlNode = baseUrlNodes[0];
			if (baseUrlNode.getNodeType() != Node.ELEMENT_NODE) {
				logger.error("\"BaseURL\" element in dash media presentation description file is invalid.");
				return false;
			}
			Element baseUrlElement = (Element) baseUrlNode;
			baseUrlElement.setTextContent(value);
			return true;
		}
		
		public int h;
		public int aBitrate;
		public int vBitrate;
		public double fr; // the frame rate that the output file should be
		public int qualityDefinitionId;
		public java.io.File outputFile;
		public boolean creatingDashAndHlsRenders = false;
	}
	
	private class VideoOutputFile {
		
		public int id;
		public int w;
		public int h;
		
		// the following may be null
		public int qualityDefinitionId;
		public Integer dashMediaPresentationDescriptionId;
		public Integer dashAudioChannelId;
		public Integer dashVideoChannelId;
		public Integer hlsPlaylistFileId;
		public Integer hlsSegmentFileId;
		
		public VideoOutputFile(File file, int w, int h, int qualityDefinitionId, File dashMediaPresentationDescriptionFile, File dashAudioChannelFile, File dashVideoChannelFile, File hlsPlaylistFile, File hlsSegmentFile) {
			this.id = file.getId();
			this.w = w;
			this.h = h;
			this.qualityDefinitionId = qualityDefinitionId;
			this.dashMediaPresentationDescriptionId = dashMediaPresentationDescriptionFile != null ? dashMediaPresentationDescriptionFile.getId() : null;
			this.dashAudioChannelId = dashAudioChannelFile != null ? dashAudioChannelFile.getId() : null;
			this.dashVideoChannelId = dashVideoChannelFile != null ? dashVideoChannelFile.getId() : null;
			this.hlsPlaylistFileId = hlsPlaylistFile != null ? hlsPlaylistFile.getId() : null;
			this.hlsSegmentFileId = hlsSegmentFile != null ? hlsSegmentFile.getId() : null;
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
