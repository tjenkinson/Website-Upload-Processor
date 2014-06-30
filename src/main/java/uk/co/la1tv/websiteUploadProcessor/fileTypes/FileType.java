package uk.co.la1tv.websiteUploadProcessor.fileTypes;

public enum FileType {
	COVER_ART(new CoverArtFileType(3));
	
	private FileTypeAbstract instance;
	
	private FileType(FileTypeAbstract a) {
		instance = a;
	}
	
	public FileTypeAbstract getObj() {
		return instance;
	}
	
	/**
	 * Get a FileType object (object extending FileTypeAbstract) corresponding to the id.
	 * @param id
	 * @return Something extending FileTypeAbstract which represents a file type.
	 */
	public static FileTypeAbstract getFromId(int id) {
		for (FileType a : FileType.values()) {
			if (a.getObj().getId() == id) {
				return a.getObj();
			}
		}
		throw(new RuntimeException("File type object could not be found for id "+id+"."));
	}
}
