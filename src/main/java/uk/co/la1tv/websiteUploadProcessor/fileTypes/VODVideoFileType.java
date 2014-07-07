package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegFileInfo;
import uk.co.la1tv.websiteUploadProcessor.helpers.FfmpegHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(java.io.File source, java.io.File workingDir, File file) {
		Config config = Config.getInstance();
		int exitVal;
		FileTypeProcessReturnInfo returnVal = new FileTypeProcessReturnInfo();
		// ids of files that should be marked in_use when the process_state is updated at the end of processing
		returnVal.fileIdsToMarkInUse = new HashSet<Integer>();
		FfmpegFileInfo info;
		
		DbHelper.updateStatus(file.getId(), "Checking video format.", null);
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
		List<Object> allFormats = config.getList("encoding.formats");
		
		ArrayList<Format> formats = new ArrayList<Format>();
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int qualityDefinitionId = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			h += h%2; // height (and width) must be multiple of 2 for libx codec
			int aBitrate = Integer.parseInt(a[2]);
			int vBitrate = Integer.parseInt(a[3]);
			if (h > sourceFileH) {
				// there's no point rendering to versions with a larger height than the source file
				logger.debug("Not rendering height "+h+" because it is more than the source file's height.");
				continue;
			}
			formats.add(new Format(qualityDefinitionId, h, aBitrate, vBitrate));
		}
		
		ArrayList<OutputFile> outputFiles = new ArrayList<OutputFile>();
		
		Connection dbConnection = DbHelper.getMainDb().getConnection();
		// loop through different formats and render videos for ones that are applicable
		for (Format f : formats) {
			
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
					return returnVal;
				}
			} catch (SQLException e) {
				throw(new RuntimeException("SQL error when trying to check if file still hasn't been deleted."));
			}
			
			logger.debug("Executing ffmpeg for height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps.");

			DbHelper.updateStatus(file.getId(), "Rendering video into required formats.", null);
			String outputFileLocation = FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+f.h;
			exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-i", source.getAbsolutePath(), "-vf", "scale=trunc(oh/a/2)*2:"+f.h, "-strict", "experimental", "-acodec", "aac", "-ab", f.aBitrate+"k", "-ac", "2", "-ar", "48000", "-vcodec", "libx264", "-vprofile", "main", "-g", "48", "-b:v", f.vBitrate+"k", "-f", "mp4", outputFileLocation}, workingDir, null, null);
			if (exitVal == 0) {
				logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
			}
			else {
				logger.warn("ffmpeg finished but returned error code "+exitVal+".");
				// if any renders fail fail the whole thing.
				// already rendered files will be cleaned up later because the working directory is cleared
				returnVal.msg = "Error rendering video.";
				return returnVal;
			}
		}
		
		
		// this order is important to make sure if anything goes wrong there aren't any files left in the webapp files folder without a corresponding entry in the db
		
		// create entries in Files with in_use set to 0
		// and copy files across to web app

		DbHelper.updateStatus(file.getId(), "Finalizing renders.", null);
		try {
			for (Format f : formats) {
				
				java.io.File outputFile = new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+f.h);
				long size = outputFile.length(); // size of file in bytes
				
				logger.debug("Creating file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");

				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,process_state) VALUES(0,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
				s.setTimestamp(1, currentTimestamp);
				s.setTimestamp(2, currentTimestamp);
				s.setLong(3, size);
				s.setInt(4, FileType.VOD_VIDEO_RENDER.getObj().getId());
				s.setInt(5, file.getId());
				if (s.executeUpdate() != 1) {
					logger.warn("Error occurred when creating database entry for a file.");
					return returnVal;
				}
				ResultSet generatedKeys = s.getGeneratedKeys();
				generatedKeys.next();
				f.id = generatedKeys.getInt(1);
				logger.debug("File record created with id "+f.id+" for render with height "+f.h+" belonging to source file with id "+file.getId()+".");
				
				// add to set of files to mark in_use when processing completed
				returnVal.fileIdsToMarkInUse.add(f.id);
				
				// add entry to OutputFiles array which will be used to populate VideoFiles table later
				// get width and height of output
				info = FfmpegHelper.getFileInfo(outputFile, workingDir);
				if (info == null) {
					logger.warn("Error retrieving info for file rendered from source file with id "+file.getId()+".");
					return returnVal;
				}
				outputFiles.add(new OutputFile(f.id, info.getW(), info.getH(), f.qualityDefinitionId));
				
				// copy file to server
				logger.info("Moving output file with id "+f.id+" to web app...");
				try {
					FileUtils.copyFile(outputFile, new java.io.File(FileHelper.format(config.getString("files.webappFilesLocation")+"/"+f.id)));
				} catch (IOException e) {
					throw(new RuntimeException("Error moving output file to webapp."));
				}
				logger.info("Output file with id "+f.id+" moved to web app.");
			}
		} catch (SQLException e) {
			throw(new RuntimeException("Error trying to register files in database."));
		}
		
		try {
			// create entries in video_files
			logger.debug("Creating entries in video_files table...");
			for (OutputFile o : outputFiles) {
				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO video_files (width,height,created_at,updated_at,quality_definition_id,file_id) VALUES (?,?,?,?,?,?)");
				s.setInt(1, o.w);
				s.setInt(2, o.h);
				s.setTimestamp(3, currentTimestamp);
				s.setTimestamp(4, currentTimestamp);
				s.setInt(5, o.qualityDefinitionId);
				s.setInt(6, o.id);
				if (s.executeUpdate() != 1) {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					logger.debug("Error registering file with id "+o.id+" in video_files table. Rolled back transaction.");
					return returnVal;
				}
				logger.debug("Created entry in video_files table for file with id "+o.id+".");
			}
			logger.debug("Created entries in video_files table.");
		} catch (SQLException e) {
			throw(new RuntimeException("Error trying to create entries in video_files."));
		}
		returnVal.success = true;
		return returnVal;
	}
		
	private class Format {
		
		public Format(int qualityDefinitionId, int h, int aBitrate, int vBitrate) {
			this.qualityDefinitionId = qualityDefinitionId;
			this.h = h;
			this.aBitrate = aBitrate;
			this.vBitrate = vBitrate;
		}
		
		public int h;
		public int aBitrate;
		public int vBitrate;
		public int qualityDefinitionId;
		public int id;
	}
	
	private class OutputFile {
		
		public int id;
		public int w;
		public int h;
		public int qualityDefinitionId;
		
		public OutputFile(int id, int w, int h, int qualityDefinitionId) {
			this.id = id;
			this.w = w;
			this.h = h;
			this.qualityDefinitionId = qualityDefinitionId;
		}
	}
}
