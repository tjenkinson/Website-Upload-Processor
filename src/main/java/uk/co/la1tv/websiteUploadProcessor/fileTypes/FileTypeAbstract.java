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
	
	/**
	 * Called when a file is ready to be processed.
	 * @param source: A io file object representing the source file's location.
	 * @param workingDir: A io file object representing the location of this files working dir.
	 * @param file: The actual file object.
	 */
	public abstract void process(java.io.File source, java.io.File workingDir, File file);
}
