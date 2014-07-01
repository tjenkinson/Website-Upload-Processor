package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeAbstract;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
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
		String fileWorkingDir = FileHelper.getFileWorkingDir(getId());
		String sourceFilePath = FileHelper.getSourceFilePath(getId());
		
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
		if (!success) {
			logger.warn("An error occurred when trying to process file with id "+getId()+".");
		}
		
		// update process_state in db
		logger.debug("Updating process_state in database...");
		try {
			Connection dbConnection = DbHelper.getMainDb().getConnection();
			PreparedStatement s;
			s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=?");
			s.setInt(1, getId());
			ResultSet r = s.executeQuery();
			if (!r.next()) {
				logger.error("Record could not be found in database for file with id "+getId()+".");
			}
			// check that ready_for_delete is still 0.
			else {
				if (r.getInt("ready_for_delete") == 1) {
					logger.debug("The file with id "+getId()+"has been processed but during this time it has been marked for deletion. Updating process_state anyway.");
				}
				// update process_state
				s = dbConnection.prepareStatement("UPDATE files SET process_state=? WHERE id=?");
				s.setInt(1, success ? 1 : 2); // a value of 1 represents success, 2 represents failure
				s.setInt(2, getId());
				if (s.executeUpdate() != 1) {
					logger.error("Error occurred updating process_state for file with id "+getId()+".");
				}
				else {
					logger.debug("Updated process_state in database.");
				}
			}
		} catch (SQLException e) {
			logger.error("Error querying database in order to update file process_state for file with id "+getId()+".");
		}
		
		
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
