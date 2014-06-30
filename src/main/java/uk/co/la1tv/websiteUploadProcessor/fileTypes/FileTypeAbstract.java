package uk.co.la1tv.websiteUploadProcessor.fileTypes;

public abstract class FileTypeAbstract {
	
	private final int id;
	
	public FileTypeAbstract(int id) {
		this.id = id;
	}
	
	public final int getId() {
		return id;
	}
}
