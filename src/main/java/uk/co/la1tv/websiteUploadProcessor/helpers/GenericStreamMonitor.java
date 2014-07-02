package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class GenericStreamMonitor implements StreamMonitor {
	
	private static Logger logger = Logger.getLogger(GenericStreamMonitor.class);
	
	private InputStream stream;
	private ArrayList<String> lines = new ArrayList<String>();
	private Object lock = new Object();
	
	@Override
	public void setStream(InputStream stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		logger.trace("GenericStreamMonitor started.");
		InputStreamReader isr =  new InputStreamReader(stream);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		try {
			while((line = br.readLine()) != null) {
				logger.trace(line);
				synchronized(lock) {
					lines.add(line);
				}
			}
		} catch (IOException e) {
			throw(new RuntimeException("Error occured when trying to read stream."));
		}	
		logger.trace("GenericStreamMonitor finished.");
	}
	
	public String[] getOutput() {
		synchronized(lock) {
			return lines.toArray(new String[lines.size()]);
		}
	}

}
