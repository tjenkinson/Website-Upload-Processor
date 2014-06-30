package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class CoverImageRenderFileType extends FileTypeAbstract {
	
	public CoverImageRenderFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverImageRenderFileType.class);
	
	public void process(File file) {
		
	}
}
