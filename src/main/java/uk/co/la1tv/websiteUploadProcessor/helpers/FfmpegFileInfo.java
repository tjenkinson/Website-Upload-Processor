package uk.co.la1tv.websiteUploadProcessor.helpers;

public class FfmpegFileInfo {
	private int w;
	private int h;
	private double duration;
	private double noFrames;
	
	public FfmpegFileInfo(int w, int h, double duration, double noFrames) {
		this.w = w;
		this.h = h;
		this.duration = duration;
		this.noFrames = noFrames;
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
	
	public double getNoFrames() {
		return noFrames;
	}
}
