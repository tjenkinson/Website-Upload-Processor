package uk.co.la1tv.websiteUploadProcessor;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;

public class File {
	private int id;
	private String name;
	private int size;
	private FileType type;
	
	public File(int id, String name, int size, FileType type) {
		this.id = id;
		this.name = name;
		this.size = size;
		this.type = type;
	}
	
	public int getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public int getSize() {
		return size;
	}
	
	public FileType getType() {
		return type;
	}
	
	public void process() {
		
	}
}
