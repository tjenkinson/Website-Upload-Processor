package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.RuntimeHelper;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public boolean process(java.io.File source, java.io.File workingDir, File file) {
		Config config = Config.getInstance();
		
		logger.debug("Starting ffmpeg...");
		int exitVal = RuntimeHelper.executeProgram(config.getString("ffmpeg.location")+" -version", workingDir);
		if (exitVal == 0) {
			logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
		}
		else {
			logger.warn("ffmpeg finished but returned error code "+exitVal+".");
		}
		
		return exitVal == 0;
	}

}
