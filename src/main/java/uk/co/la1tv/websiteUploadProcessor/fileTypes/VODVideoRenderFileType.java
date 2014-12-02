package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class VODVideoRenderFileType extends FileTypeAbstract {
	
	public VODVideoRenderFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(VODVideoRenderFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(final Connection dbConnection, java.io.File source, java.io.File workingDir, File file) {
		// TODO Auto-generated method stub
		return null;
	}
}
