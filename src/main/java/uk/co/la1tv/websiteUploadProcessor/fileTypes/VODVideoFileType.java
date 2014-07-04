package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
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
		
		// TODO: tmp for testing
		if (true)
		return false;
		
		// get source file information. Get width and height and check that duration is more than 0
		
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
			String[] a = ((String) f).split("-"); // check second arg
			int h = Integer.parseInt(a[0]);
			h += h%2; // height (and width) must be multiple of 2 for libx codec
			int aBitrate = Integer.parseInt(a[1]);
			int vBitrate = Integer.parseInt(a[2]);
			
			if (h > sourceFileH) {
				// there's no point rendering to versions with a larger height than the source file
				logger.debug("Not rendering height "+h+" because it is more than the source file's height.");
				continue;
			}
			
			logger.debug("Executing ffmpeg for height "+h+" and audio bitrate "+aBitrate+"kbps, video bitrate "+vBitrate+"kbps.");
			exitVal = RuntimeHelper.executeProgram("\""+config.getString("ffmpeg.location")+"\" -y -nostdin -i \""+source.getAbsolutePath()+"\" -vf scale=trunc(oh/a/2)*2:"+h+" -strict experimental -acodec aac -ab "+aBitrate+"k -ac 2 -ar 48000 -vcodec libx264 -vprofile main -g 48 -b:v "+vBitrate+"k -f mp4 output_"+h, workingDir, null, null);
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
		
		// TODO: temporary for testing
		System.exit(1);
		
		// TODO: if successful copy files to web server and create file entries in db and do the rest of the db stuff that's needed
		return success;
	}

}
