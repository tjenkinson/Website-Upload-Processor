package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageFormat;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageMagickFormat;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageProcessorHelper;

public class CoverArtImageFileType extends FileTypeAbstract {
	
	public CoverArtImageFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverArtImageFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(java.io.File source, java.io.File workingDir, File file) {
		// any image format and size is acceptable. It will always be cropped to become correct aspect ratio
		Config config = Config.getInstance();
		FileTypeProcessReturnInfo returnVal = new FileTypeProcessReturnInfo();
		// ids of files that should be marked in_use when the process_state is updated at the end of processing
		returnVal.fileIdsToMarkInUse = new HashSet<Integer>();
		List<Object> allFormats = config.getList("encoding.coverArtImageFormats");
		
		DbHelper.updateStatus(file.getId(), "Processing image.", null);
		
		ImageMagickFormat inputFormat = ImageMagickFormat.getFormatFromExtension(file.getExtension());
		if (inputFormat == null) {
			logger.warn("Error occurred when trying to get image magick formar from image file with id "+file.getId()+".");
			returnVal.msg = "Error trying to determine image format.";
			return returnVal;
		}
		
		final ArrayList<ImageFormat> formats = new ArrayList<ImageFormat>();
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int w = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			formats.add(new ImageFormat(w, h, new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/output_"+w+"_"+h))));
		}
		
		returnVal.success = ImageProcessorHelper.process(returnVal, source, workingDir, formats, inputFormat, ImageMagickFormat.JPG, file);
		return returnVal;
	}
	
}
