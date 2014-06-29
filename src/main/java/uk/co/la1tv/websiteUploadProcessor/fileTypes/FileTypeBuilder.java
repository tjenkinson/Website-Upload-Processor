package uk.co.la1tv.websiteUploadProcessor.fileTypes;

public class FileTypeBuilder {
	
	/**
	 * Get a FileType object corresponding to the file type id from the db
	 * @param id
	 * @return FileType object
	 */
	public static FileType build(int id) {
		FileType type = null;
		
		if (id == 1) {
			type = new CoverArtFileType();
		}
		else {
			// TODO: log error
		}
		return type;
	}
}
