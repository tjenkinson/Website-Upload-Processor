package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.File;

public class HlsMediaPlaylistType extends FileTypeAbstract {
	
	public HlsMediaPlaylistType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(HlsMediaPlaylistType.class);

	@Override
	public FileTypeProcessReturnInfo process(final Connection dbConnection, java.io.File source, java.io.File workingDir, File file, final boolean workingWithCopy) {
		// TODO Auto-generated method stub
		return null;
	}
}
