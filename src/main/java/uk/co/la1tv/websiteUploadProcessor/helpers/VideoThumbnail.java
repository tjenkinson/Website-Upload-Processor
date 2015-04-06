package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;

public class VideoThumbnail {
	private final File file;
	private final int time;

	public VideoThumbnail(int time, File file) {
		this.file = file;
		this.time = time;
	}
	
	// get the time into the video that this thumbnail corresponds to
	public int getTime() {
		return time;
	}
	
	// get the file for this thumbnail
	public File getFile() {
		return file;
	}
}
