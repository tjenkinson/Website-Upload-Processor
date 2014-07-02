package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.GenericStreamMonitor;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.StreamMonitor;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public boolean process(java.io.File source, java.io.File workingDir, File file) {
		Config config = Config.getInstance();
		int exitVal;
		
		// TODO: get source file information
		
		GenericStreamMonitor streamMonitor = new GenericStreamMonitor();
		
		exitVal = RuntimeHelper.executeProgram(config.getString("ffmpeg.location")+" -version", workingDir, streamMonitor, null);
		
		
		//1080-4000, 720-2500, 480-700, 360-500
		// TODO: check if this can be replaced with String list instead of Object one
		List<Object> formats = config.getList("encoding.formats");
		
		// loop through different formats and render videos for ones that are applicable
		for (Object f : formats) {	
			
			
		}
		
		
		
		
		logger.debug("Starting ffmpeg...");
		exitVal = RuntimeHelper.executeProgram(config.getString("ffmpeg.location")+" -version", workingDir, null, null);
		if (exitVal == 0) {
			logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
		}
		else {
			logger.warn("ffmpeg finished but returned error code "+exitVal+".");
		}
		
		return exitVal == 0;
	}

}
