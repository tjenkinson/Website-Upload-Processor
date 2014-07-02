package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.InputStream;

public interface StreamMonitor extends Runnable {
	
	public void setStream(InputStream stream);
	
}
