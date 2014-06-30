package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class SideBannersImageFileType extends FileTypeAbstract {
	
	public SideBannersImageFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(SideBannersImageFileType.class);
	
	public void process(File file) {
		
	}
}
