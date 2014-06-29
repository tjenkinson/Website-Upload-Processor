package uk.co.la1tv.websiteUploadProcessor.helpers;

public class FileHelper {
	
	private FileHelper() {}
	
	/**
	 * Formats the path passed in so that it is correct for the filesystem it's running on. 
	 * @param path
	 * @return Formatted path
	 */
	public static String format(String path) {
		char sep = System.getProperty("file.separator").charAt(0);
		return path.replace(sep == '\\' ? '/' : '\\', sep);
	}
}
