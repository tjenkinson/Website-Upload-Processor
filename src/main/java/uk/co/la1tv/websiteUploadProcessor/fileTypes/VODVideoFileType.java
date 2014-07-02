package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.io.IOException;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.StreamGobbler;
import uk.co.la1tv.websiteUploadProcessor.helpers.StreamType;

public class VODVideoFileType extends FileTypeAbstract {
	
	public VODVideoFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoFileType.class);

	@Override
	public boolean process(java.io.File source, java.io.File workingDir, File file) {
		Config config = Config.getInstance();
		
		logger.debug("Starting ffmpeg...");
		int exitVal;
		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(config.getString("ffmpeg.location"), null, workingDir);
			// consume the err stream and stdout stream from the process
			new StreamGobbler(proc.getErrorStream(), StreamType.ERR).start();
			new StreamGobbler(proc.getInputStream(), StreamType.STDOUT).start(); // the input stream is the STDOUT from the program being executed
			exitVal = proc.waitFor();
		
		} catch (IOException e) {
			throw(new RuntimeException("Error trying to execute ffmpeg."));
		} catch (InterruptedException e) {
			throw(new RuntimeException("InterruptException occured. This shouldn't happen."));
		}
		
		if (exitVal == 0) {
			logger.debug("ffmpeg finished successfully with error code "+exitVal+".");
		}
		else {
			logger.warn("ffmpeg finished but returned error code "+exitVal+".");
		}
		
		return exitVal == 0;
	}

}
