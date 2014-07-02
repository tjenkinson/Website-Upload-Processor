package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.InputStream;

public abstract class StreamMonitor implements Runnable {
	
	public abstract void setStream(InputStream stream);
	
}
