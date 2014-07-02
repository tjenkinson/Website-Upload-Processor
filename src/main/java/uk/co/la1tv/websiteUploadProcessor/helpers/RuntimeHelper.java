package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.VODVideoFileType;

public class RuntimeHelper {
	
	private static Logger logger = Logger.getLogger(RuntimeHelper.class);
	
	/**
	 * Execute the program passed in and return the exit code.
	 * @param path: Path to program to run.
	 * @param workingDir: The working directory the program should use.
	 * @return The exit code from the program being run.
	 */
	public static int executeProgram(String path, File workingDir) {
		logger.trace("Executing '"+path+"' with working dir '"+workingDir.getAbsolutePath()+"'.");
		int exitVal;
		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(path, null, workingDir);
			// consume the err stream and stdout stream from the process
			new StreamGobbler(proc.getErrorStream(), StreamType.ERR).start();
			new StreamGobbler(proc.getInputStream(), StreamType.STDOUT).start(); // the input stream is the STDOUT from the program being executed
			exitVal = proc.waitFor();
		
		} catch (IOException e) {
			throw(new RuntimeException("Error trying to execute program."));
		} catch (InterruptedException e) {
			throw(new RuntimeException("InterruptException occured. This shouldn't happen."));
		}
		return exitVal;
	}
}
