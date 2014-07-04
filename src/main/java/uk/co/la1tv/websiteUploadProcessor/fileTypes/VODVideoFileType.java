package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.GenericStreamMonitor;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public boolean process(java.io.File source, java.io.File workingDir, File file) {
		Config config = Config.getInstance();
		int exitVal;
		
		// get source file information.
		GenericStreamMonitor streamMonitor = new GenericStreamMonitor();
		
		exitVal = RuntimeHelper.executeProgram("\""+config.getString("ffmpeg.probeLocation")+"\" -v quiet -print_format json -show_format -show_streams \""+source.getAbsolutePath()+"\"", workingDir, streamMonitor, null);
		if (exitVal != 0) {
			logger.warn("Cannot process VOD file with id "+file.getId()+" because there is an error in the metadata.");
			return false;
		}
		
		// the output from ffmpegprobe should be pure json
		JSONObject metadata;
		
		String tmp = streamMonitor.getOutput();
		try {
			
			metadata = new JSONObject(tmp);
		}
		catch(JSONException e) {
			logger.warn("Error parsing JSON from ffmpegprobe.");
			System.out.println(tmp);
			System.exit(1); // TODO: remove
			return false;
		}
		
		// check the duration is more than 0
		if (Double.parseDouble(metadata.getJSONObject("format").getString("duration")) == 0) {
			logger.warn("Cannot process VOD file with id "+file.getId()+" because it's duration is 0.");
			return false;
		}
		
		// get video height
		int sourceFileH = metadata.getJSONArray("streams").getJSONObject(0).getInt("height");
		List<Object> allFormats = config.getList("encoding.formats");
		
		ArrayList<Format> formats = new ArrayList<Format>();
		
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int fileTypeId = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			h += h%2; // height (and width) must be multiple of 2 for libx codec
			int aBitrate = Integer.parseInt(a[2]);
			int vBitrate = Integer.parseInt(a[3]);
			if (h > sourceFileH) {
				// there's no point rendering to versions with a larger height than the source file
				logger.debug("Not rendering height "+h+" because it is more than the source file's height.");
				continue;
			}
			formats.add(new Format(fileTypeId, h, aBitrate, vBitrate));
		}
		
		Connection dbConnection = DbHelper.getMainDb().getConnection();
		boolean success = true;
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
					return false;
				}
			} catch (SQLException e) {
				throw(new RuntimeException("SQL error when trying to check if file still hasn't been deleted."));
			}
			
			
			logger.debug("Executing ffmpeg for height "+f.h+" and audio bitrate "+f.aBitrate+"kbps, video bitrate "+f.vBitrate+"kbps.");
			
			String outputFileLocation = FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+f.h;
			exitVal = RuntimeHelper.executeProgram("\""+config.getString("ffmpeg.location")+"\" -y -nostdin -i \""+source.getAbsolutePath()+"\" -vf scale=trunc(oh/a/2)*2:"+f.h+" -strict experimental -acodec aac -ab "+f.aBitrate+"k -ac 2 -ar 48000 -vcodec libx264 -vprofile main -g 48 -b:v "+f.vBitrate+"k -f mp4 \""+outputFileLocation+"\"", workingDir, null, null);
			if (exitVal == 0) {
				logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
			}
			else {
				logger.warn("ffmpeg finished but returned error code "+exitVal+".");
				// if any renders fail fail the whole thing.
				// already rendered files will be cleaned up later because the working directory is cleared
				success = false;
				break;
			}
			
		}
		
		
		// this order is important to make sure if anything goes wrong there aren't any files left in the webapp files folder without a corresponding entry in the db
		
		// create entries in Files with in_use set to 0
		// and copy files accross to web app
		try {
			for (Format f : formats) {
				
				java.io.File outputFile = new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+f.h);
				long size = outputFile.length(); // size of file in bytes
				
				logger.debug("Creating file record for render with height "+f.h+" belonging to source file with id "+file.getId());

				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,process_state) VALUES(0,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
				s.setTimestamp(1, currentTimestamp);
				s.setTimestamp(2, currentTimestamp);
				s.setLong(3, size);
				s.setInt(4, f.fileTypeId);
				s.setInt(5, file.getId());
				if (s.executeUpdate() != 1) {
					logger.warn("Error occurred when creating database entry for a file.");
					return false;
				}
				ResultSet generatedKeys = s.getGeneratedKeys();
				generatedKeys.next();
				f.id = generatedKeys.getInt(1);
				logger.info("File record created with id "+f.id+" for render with height "+f.h+" belonging to source file with id "+file.getId());
				
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
			logger.debug("Marking rendered file with id as "+file.getId()+" as in_use.");

			// start transaction
			dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
			for (Format f : formats) {
				PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET in_use=1 WHERE id=?");
				s.setInt(1, f.id);
				
				if (s.executeUpdate() != 1) {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					logger.debug("Error marking rendered file with id as "+file.getId()+" as in_use. Rolled back transaction.");
					return false;
				}
			}
			//TODO: still need to create entry in video_files here
			dbConnection.prepareStatement("COMMIT").executeUpdate();
		} catch (SQLException e) {
			// rollback transaction
			try {
				dbConnection.prepareStatement("ROLLBACK").executeUpdate();
			} catch (SQLException e1) {
				logger.trace("Transaction failed to be rolled back. Can happen if transaction failed to start.");
			}
			e.printStackTrace();
			throw(new RuntimeException("Error trying to register files in database."));
		}
		
		logger.debug("Marked all files as in_use and created entry in video_files table.");
		
		return success;
	}
		
	private class Format {
		
		public Format(int fileTypeId, int h, int aBitrate, int vBitrate) {
			this.fileTypeId = fileTypeId;
			this.h = h;
			this.aBitrate = aBitrate;
			this.vBitrate = vBitrate;
		}
		
		public int h;
		public int aBitrate;
		public int vBitrate;
		public int fileTypeId;
		public int id;
	}
}
