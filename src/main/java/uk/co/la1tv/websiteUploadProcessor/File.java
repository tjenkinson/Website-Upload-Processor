package uk.co.la1tv.websiteUploadProcessor;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeAbstract;

public class File {
	
	private static Logger logger = Logger.getLogger(File.class);
	
	private int id;
	private String name;
	private int size;
	private FileTypeAbstract type;
	
	public File(int id, String name, int size, FileTypeAbstract type) {
	
		this.id = id;
		this.name = name;
		this.size = size;
		this.type = type;
		logger.info("Created File object for file with id "+id+" and name '"+name+"'.");
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
	
	public FileTypeAbstract getType() {
		return type;
	}
	
	public void process() {
		logger.info("Started processing file with id "+id+" and name '"+name+"'.");
	
		
		logger.info("Finished processing file with id "+id+" and name '"+name+"'.");
	}
}
