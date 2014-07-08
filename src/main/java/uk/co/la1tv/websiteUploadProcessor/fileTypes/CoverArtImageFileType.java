package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
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
		
		final ArrayList<Format> formats = new ArrayList<Format>();
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int w = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			formats.add(new Format(w, h));
		}
		
		for (Format f : formats) {
			logger.debug("Executing ffmpeg to process image file for source file with width "+f.w+" and height "+f.h+".");
			java.io.File outputFile =  new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/output_"+f.w+"_"+f.h));
			int exitVal = ImageProcessorHelper.process(source, outputFile, workingDir, f.w, f.h);
			if (exitVal != 0) {
				logger.warn("ffmpeg finished processing image but returned error code "+exitVal+".");
				returnVal.msg = "Error processing image.";
				return returnVal;
			}
		}
		
		
		
		
		returnVal.success = true;
		return returnVal;
	}
	
	private class Format {
		
		public Format(int w, int h) {
			this.w = w;
			this.h = h;
		}
		
		public int w;
		public int h;
	}
	
	private class OutputFile {
		
		public int id;
		public int w;
		public int h;
		public int qualityDefinitionId;
		
		public OutputFile(int id, int w, int h, int qualityDefinitionId) {
			this.id = id;
			this.w = w;
			this.h = h;
			this.qualityDefinitionId = qualityDefinitionId;
		}
	}
	
	
}
