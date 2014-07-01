package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class CoverArtImageRenderFileType extends FileTypeAbstract {
	
	public CoverArtImageRenderFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverArtImageRenderFileType.class);

	@Override
	public boolean process(java.io.File source, java.io.File workingDir, File file) {
		// TODO Auto-generated method stub
		return false;
	}
	
}
