package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;

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
	 * @param dbConnection: a Connection object which provides access to the database.
	 * @param source: A io file object representing the source file's location.
	 * @param workingDir: A io file object representing the location of this files working dir.
	 * @param file: The actual file object.
	 * @param workingWithCopy: true if the source file is a copy of the original, not the original.
	 * @return null if error processing or set of ids to mark as in_use when process is marked as completed
	 */
	public abstract FileTypeProcessReturnInfo process(Connection dbConnection, java.io.File source, java.io.File workingDir, File file, boolean workingWithCopy);
}
