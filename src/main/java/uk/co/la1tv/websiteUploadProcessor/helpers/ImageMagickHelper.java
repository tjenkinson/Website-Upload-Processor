package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;

public class ImageMagickHelper {

	private static Logger logger = Logger.getLogger(ImageMagickHelper.class);
	
	public static ImageMagickFileInfo getFileInfo(String fileFormat, File file, File workingDir) {
		Config config = Config.getInstance();
		int exitVal;
		
		// get source file information.
		GenericStreamMonitor streamMonitor = new GenericStreamMonitor();
		
		exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("imagemagick.identifyLocation"), "-format", "%w,%h", fileFormat+":"+file.getAbsolutePath()}, workingDir, streamMonitor, null);
		if (exitVal != 0) {
			logger.warn("Error retrieving metadata for file '"+file.getAbsolutePath()+"' with ImageMagick identify.");
			return null;
		}
		
		String[] values = streamMonitor.getOutput().split("\n")[0].split(",");

		int w = Integer.parseInt(values[0]);
		int h = Integer.parseInt(values[1]);
		
		return new ImageMagickFileInfo(w, h);
	}
}
