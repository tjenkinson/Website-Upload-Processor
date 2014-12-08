package uk.co.la1tv.websiteUploadProcessor.helpers;

public class ImageFormat {
	
	public ImageFormat(int w, int h, java.io.File outputFile) {
		this.w = w;
		this.h = h;
		this.outputFile = outputFile;
	}
	
	public int w;
	public int h;
	public java.io.File outputFile;
}