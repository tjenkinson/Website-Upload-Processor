package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
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
		JSONObject metadata = new JSONObject(streamMonitor.getOutput());
		
		// check the duration is more than 0
		if (Double.parseDouble(metadata.getJSONObject("format").getString("duration")) == 0) {
			logger.warn("Cannot process VOD file with id "+file.getId()+" because it's duration is 0.");
			return false;
		}
		
		// get video height
		int sourceFileH = metadata.getJSONArray("streams").getJSONObject(0).getInt("height");
		List<Object> formats = config.getList("encoding.formats");
		
		boolean success = true;
		// loop through different formats and render videos for ones that are applicable
		for (Object f : formats) {
			
			// TODO: check if file is now marked for deletion
			
			String[] a = ((String) f).split("-");
			int h = Integer.parseInt(a[1]);
			h += h%2; // height (and width) must be multiple of 2 for libx codec
			int aBitrate = Integer.parseInt(a[2]);
			int vBitrate = Integer.parseInt(a[3]);
			
			if (h > sourceFileH) {
				// there's no point rendering to versions with a larger height than the source file
				logger.debug("Not rendering height "+h+" because it is more than the source file's height.");
				continue;
			}
			
			logger.debug("Executing ffmpeg for height "+h+" and audio bitrate "+aBitrate+"kbps, video bitrate "+vBitrate+"kbps.");
			
			String outputFileLocation = FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+h;
			//exitVal = RuntimeHelper.executeProgram("\""+config.getString("ffmpeg.location")+"\" -y -nostdin -i \""+source.getAbsolutePath()+"\" -vf scale=trunc(oh/a/2)*2:"+h+" -strict experimental -acodec aac -ab "+aBitrate+"k -ac 2 -ar 48000 -vcodec libx264 -vprofile main -g 48 -b:v "+vBitrate+"k -f mp4 \""+outputFileLocation+"\"", workingDir, null, null);
			exitVal = 0; // TODO: for testing only
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
		// - create entries in Files with in_use set to 0
		Connection dbConnection = DbHelper.getMainDb().getConnection();
		int[] newFileIds = new int[formats.size()];
		
		try {
		
			for (int i=0; i<formats.size(); i++) {
				Object f = formats.get(i);
				
				String[] a = ((String) f).split("-");
				
				// TODO: these are repeated in both loops. could be cleaned up
				int fileTypeId = Integer.parseInt(a[0]);
				int h = Integer.parseInt(a[1]);
				java.io.File outputFile = new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/")+"output_"+h);
				long size = outputFile.length(); // size of file in bytes
				
				PreparedStatement s;
				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				// TODO: created_at and updated_at
				s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,process_state) VALUES(0,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
				s.setTimestamp(1, currentTimestamp);
				s.setTimestamp(2, currentTimestamp);
				s.setLong(3, size);
				s.setInt(4, fileTypeId);
				s.setInt(5, file.getId());
				if (s.executeUpdate() != 1) {
					logger.warn("Error occurred when creating database entry for a file.");
					return false;
				}
				ResultSet generatedKeys = s.getGeneratedKeys();
				generatedKeys.next();
				int newId = generatedKeys.getInt(1);
				newFileIds[i] = newId;
				logger.info("File record created with id "+newId+" for render with height "+h+" belonging to source file with id "+file.getId());
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw(new RuntimeException("Error trying to register files in database."));
		}
		

		// TODO: temporary for testing
		System.exit(1);
		
		// - copy files across to web server
		
		
		// - check files there
		
		
		// - start transaction
		
		// - update in_use to 1 for new files (in transaction so if anything fails, all the files will be deleted)
		// - create entry in video_files table
		// - update process_state
		// - commit transaction
		
		// TODO: if successful copy files to web server and create file entries in db and do the rest of the db stuff that's needed
		return success;
	}

}
