package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import uk.co.la1tv.websiteUploadProcessor.File;

public abstract class FileTypeAbstract {
	
	private final int id;
	
	/**
	 * Represents a file type in the database.
	 * @param id: The id of the file type in the database.
	 */
	public FileTypeAbstract(int id) {
		this.id = id;
	}
	
	public final int getId() {
		return id;
	}
	
	public abstract void process(File file);
}
