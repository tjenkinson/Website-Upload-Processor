package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/**
 * Reads an input stream and logs it. Terminates when the stream ends.
 *
 */
public class StreamGobbler extends Thread {
	
	private static Logger logger = Logger.getLogger(StreamGobbler.class);
	
	private InputStream is;
	private StreamType type;

	public StreamGobbler(InputStream is, StreamType type) {
		this.is = is;
		this.type = type;
	}
	
	@Override
	public void run() {
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		try {
			while((line = br.readLine()) != null) {
				String msg = type.name()+": "+line;
				if (type != StreamType.ERR) {
					logger.trace(msg);
				}
				else {
					logger.warn(msg);
				}
			}
		} catch (IOException e) {
			throw(new RuntimeException("Error occured when trying to read stream."));
		}
	}
}
