package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;

import uk.co.la1tv.websiteUploadProcessor.Config;

public class ImageProcessorHelper {
	
	/**
	 * Scale the image source file (mjpeg) passed in so that it fills a box of with w and height h and then crops.
	 * @param source
	 * @param destination
	 * @param workingDir
	 * @param w
	 * @param h
	 * @return exit val from ffmpeg
	 */
	public static int process(File source, File destination, File workingDir, int w, int h) {
		Config config = Config.getInstance();
		int exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-timelimit", ""+config.getInt("ffmpeg.imageEncodeTimeLimit"), "-f", "mjpeg", "-i", source.getAbsolutePath(), "-vf", "scale='if(gt(ih*(ow/iw),oh),oh,-1)':'if(gt(iw*(oh/ih),ow),ow,-1)', crop="+w+":"+h, "-f", "mjpeg", destination.getAbsolutePath()}, workingDir, null, null);
		return exitVal;
	}
}
