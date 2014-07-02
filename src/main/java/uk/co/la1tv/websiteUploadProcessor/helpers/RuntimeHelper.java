package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

public class RuntimeHelper {
	
	private static Logger logger = Logger.getLogger(RuntimeHelper.class);
	
	/**
	 * Execute the program passed in and return the exit code.
	 * @param path: Path to program to run.
	 * @param workingDir: The working directory the program should use.
	 * @param inputStreamHandler: Something implementing StreamMonitor to read the output stream from the program in a new thread. Null means discard stream.
	 * @param errStream: Something implementing StreamMonitor to read the error output of the program in a new thread. Null means discard stream.
	 * @return The exit code from the program being run.
	 */
	public static int executeProgram(String path, File workingDir, StreamMonitor inputStream, StreamMonitor errStream) {
		logger.trace("Executing '"+path+"' with working dir '"+workingDir.getAbsolutePath()+"'.");
		int exitVal;
		try {
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(path, null, workingDir);
			// consume the err stream and stdout stream from the process
			if (inputStream != null) {
				inputStream.setStream(proc.getInputStream());
				new Thread(inputStream).start();
			}
			else {
				new StreamGobbler(proc.getInputStream(), StreamType.STDOUT).start(); // the input stream is the STDOUT from the program being executed
			}
			if (errStream != null) {
				errStream.setStream(proc.getErrorStream());
				new Thread(errStream).start();
			}
			else {
				new StreamGobbler(proc.getErrorStream(), StreamType.ERR).start();
			}
	
			exitVal = proc.waitFor();
		
		} catch (IOException e) {
			throw(new RuntimeException("Error trying to execute program."));
		} catch (InterruptedException e) {
			throw(new RuntimeException("InterruptException occured. This shouldn't happen."));
		}
		return exitVal;
	}
}
