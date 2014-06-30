package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class CoverArtFileType extends FileTypeAbstract {
	
	public CoverArtFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverArtFileType.class);
	
	public void process(File file) {
		
	}
}
