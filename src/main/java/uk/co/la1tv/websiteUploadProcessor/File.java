package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeAbstract;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

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
		logger.info("Created File object for file of type '"+type.getClass().getSimpleName()+"' with id "+id+" and name '"+name+"'.");
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
		
		Config config = Config.getInstance();
		
		logger.info("Started processing file with id "+getId()+" and name '"+getName()+"'.");
		logger.debug("Creating folder for file in working directory...");
		String workingDir = FileHelper.format(config.getString("files.workingFilesLocation"));
		String fileWorkingDir = FileHelper.format(workingDir+"/"+getId());
		String sourceFilePath = FileHelper.format(config.getString("files.webappFilesLocation")+"/"+getId());
		
		try {
			FileUtils.forceMkdir(new java.io.File(fileWorkingDir));
		} catch (IOException e) {
			throw(new RuntimeException("Error creating folder for file to process."));
		}
		
		if (config.getBoolean("general.workWithCopy")) {
			try {
				FileUtils.copyFileToDirectory(new java.io.File(sourceFilePath), new java.io.File(fileWorkingDir));
			} catch (IOException e) {
				logger.error("Error copying file with id "+getId()+" from web app files location to working directory.");
				return;
			}
		}
		logger.debug("Created folder for file in working directory.");
		
		boolean success = type.process(new java.io.File(sourceFilePath), new java.io.File(fileWorkingDir), this);
		
		
		// TODO: update process_state in db
		
		
		logger.debug("Removing files working directory...");
		try {
			FileUtils.deleteDirectory(new java.io.File(fileWorkingDir));
			logger.debug("Removed files working directory.");
		} catch (IOException e) {
			logger.error("Error removing files working directory.");
		}
		
		logger.info("Finished processing file with id "+getId()+" and name '"+getName()+"'.");
	}
}
