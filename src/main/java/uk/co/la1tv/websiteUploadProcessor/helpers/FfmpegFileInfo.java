package uk.co.la1tv.websiteUploadProcessor.helpers;

public class FfmpegFileInfo {
	private int w;
	private int h;
	private double duration;
	
	public FfmpegFileInfo(int w, int h, double duration) {
		this.w = w;
		this.h = h;
		this.duration = duration;
	}
	
	public int getW() {
		return w;
	}
	
	public int getH() {
		return h;
	}
	
	public double getDuration() {
		return duration;
	}
}
