package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class CoverImageFileType extends FileTypeAbstract {
	
	public CoverImageFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverImageFileType.class);
	
	public void process(File file) {
		
	}
}
