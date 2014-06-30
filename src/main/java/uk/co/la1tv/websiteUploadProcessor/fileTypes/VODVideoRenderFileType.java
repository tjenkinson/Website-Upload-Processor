package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class VODVideoRenderFileType extends FileTypeAbstract {
	
	public VODVideoRenderFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoRenderFileType.class);
	
	public void process(File file) {
		
	}
}
