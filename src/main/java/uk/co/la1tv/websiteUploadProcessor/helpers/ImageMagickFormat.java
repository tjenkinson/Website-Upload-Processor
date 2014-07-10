package uk.co.la1tv.websiteUploadProcessor.helpers;



/**
 * Mappings between file extensions and ImageMagick file format.
 */
public enum ImageMagickFormat {
	JPG("jpg", "JPG"),
	JPEG("jpeg", "JPEG"),
	PNG("png", "PNG");
	
	private final String extension;
	private final String iMFormat;
	
	private ImageMagickFormat(String extension, String iMFormat) {
		this.extension = extension;
		this.iMFormat = iMFormat;
	}
	
	public String getExtension() {
		return extension;
	}
	
	public String getIMFormat() {
		return iMFormat;
	}
	
	/**
	 * Get the image magick format for a particular extension.
	 * @param extension
	 * @return the format object or null if a format could not be found.
	 */
	public static ImageMagickFormat getFormatFromExtension(String extension) {
		extension = extension.toLowerCase();
		for (ImageMagickFormat a : ImageMagickFormat.values()) {
			if (a.getExtension().equals(extension)) {
				return a;
			}
		}
		return null;
	}
}
