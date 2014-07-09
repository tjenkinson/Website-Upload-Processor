package uk.co.la1tv.websiteUploadProcessor.helpers;

public class ImageMagickFileInfo {
	private int w;
	private int h;
	
	public ImageMagickFileInfo(int w, int h) {
		this.w = w;
		this.h = h;
	}
	
	public int getW() {
		return w;
	}
	
	public int getH() {
		return h;
	}
}
