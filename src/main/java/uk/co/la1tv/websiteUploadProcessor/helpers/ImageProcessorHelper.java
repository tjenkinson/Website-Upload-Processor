package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;

import uk.co.la1tv.websiteUploadProcessor.Config;

public class ImageProcessorHelper {
	
	/**
	 * Scale the image source file passed in so that it fills a box of with w and height h and then crops and writes to destination.
	 * @param inputFormat: the ImageMagick format of the input file. ("identify -list format" to get supported formats)
	 * @param outputFormat: the ImageMagick format of the output file.
	 * @param source
	 * @param destination
	 * @param workingDir
	 * @param w
	 * @param h
	 * @return exit val from ImageMagick
	 */
	public static int process(ImageMagickFormat inputFormat, ImageMagickFormat outputFormat, File source, File destination, File workingDir, int w, int h) {
		Config config = Config.getInstance();
		int exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("imagemagick.convertLocation"), inputFormat.getIMFormat()+":"+source.getAbsolutePath(), "-resize", w+"x"+h+"^", "-gravity", "center", "-crop", w+"x"+h+"+0+0", outputFormat.getIMFormat()+":"+destination.getAbsolutePath()}, workingDir, null, null);
		return exitVal;
	}
}
