package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageFormat;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageMagickFormat;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageProcessorHelper;

public class CoverArtImageFileType extends FileTypeAbstract {
	
	public CoverArtImageFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverArtImageFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(final Connection dbConnection, java.io.File source, java.io.File workingDir, File file) {
		// any image format and size is acceptable. It will always be cropped to become correct aspect ratio
		Config config = Config.getInstance();
		FileTypeProcessReturnInfo returnVal = new FileTypeProcessReturnInfo();
		
		DbHelper.updateStatus(dbConnection, file.getId(), "Processing image.", null);
		
		ImageMagickFormat inputFormat = ImageMagickFormat.getFormatFromExtension(file.getExtension());
		if (inputFormat == null) {
			logger.warn("Error occurred when trying to get image magick formar from image file with id "+file.getId()+".");
			returnVal.msg = "Error trying to determine image format.";
			return returnVal;
		}
		
		final List<ImageFormat> formats = ImageProcessorHelper.getFormats(config.getList("encoding.coverArtImageFormats"), workingDir);
		returnVal.success = ImageProcessorHelper.process(dbConnection, returnVal, source, workingDir, formats, inputFormat, ImageMagickFormat.JPG, file, FileType.COVER_ART_IMAGE_RENDER);
		return returnVal;
	}
	
}
