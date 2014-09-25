package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeAbstract;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeProcessReturnInfo;
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
		logger.debug("Created File object for file of type '"+type.getClass().getSimpleName()+"' with id "+id+" and name '"+name+"'.");
	}
	
	@Override
	public boolean equals(Object a) {
		return a instanceof File && ((File) a).getId() == getId();
	}
	
	@Override
	public int hashCode() {
		return getId();
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
	
	// returns null if the file does not have an extension.
	public String getExtension() {
		String[] a = getName().split("\\.");
		return a.length > 1 ? a[a.length-1] : null;
	}
	
	public void process() {
		
		Config config = Config.getInstance();
		// used to signify error if problem copying file from pending files locatioon to server (if working with copy) or if there was a problem moving the source fie from the pending directory to the main files directory.
		boolean errorMovingSourceFile = false;
		// true if the webapp folder is over quota meaning the job should be failed.
		boolean overQuota = FileHelper.isOverQuota();
		boolean workingDirCreated = false;
		FileTypeProcessReturnInfo info = null;
		
		logger.info("Started processing file with id "+getId()+" and name '"+getName()+"'.");
		DbHelper.updateStatus(getId(), "Started processing.", null);
		
		String fileWorkingDir = FileHelper.getFileWorkingDir(getId());
		String sourceFilePath = FileHelper.getSourcePendingFilePath(getId());
		// the path to file that will be processed. (It might be copied to the working dir so this will be different to sourceFilePath)
		String destinationSourceFilePath = fileWorkingDir;
		
		if (!overQuota) {
			logger.debug("Creating folder for file in working directory...");
			
			try {
				FileUtils.forceMkdir(new java.io.File(fileWorkingDir));
			} catch (IOException e) {
				throw(new RuntimeException("Error creating folder for file to process."));
			}
			workingDirCreated = true; // Purposefully put this outside try block in case it it is partly created maybe, in which case it would still be good to attempt to delete it later.
			logger.debug("Created folder for file in working directory.");
			
			if (config.getBoolean("general.workWithCopy")) {
				try {
					destinationSourceFilePath = FileHelper.format(fileWorkingDir+"/source");
					logger.debug("Copying file with id "+getId()+" to working directory...");
					FileUtils.copyFile(new java.io.File(sourceFilePath), new java.io.File(destinationSourceFilePath));
					logger.debug("Copied file with id "+getId()+" to working directory.");
				} catch (IOException e) {
					errorMovingSourceFile = true;
					logger.error("Error copying file with id "+getId()+" from web app files location to working directory.");
				}
			}
		}
		
		if (!errorMovingSourceFile && !overQuota) {
			info = type.process(new java.io.File(destinationSourceFilePath), new java.io.File(fileWorkingDir), this);
		}
		
		if (info == null) {
			// this has success set to false
			info = new FileTypeProcessReturnInfo();
			if (overQuota) {
				info.msg = "There is no free storage space.";
				logger.warn("Could not process file with id "+getId()+" because the web app is over the storage quota.");
			}
		}
		if (!info.success) {
			logger.warn("An error occurred when trying to process file with id "+getId()+".");
		}
		
		if (!errorMovingSourceFile) {
			
			java.io.File sourceFile = new java.io.File(sourceFilePath);
			
			// check if would be over quote now when source file moved across
			if (FileHelper.isOverQuota(new BigInteger(""+sourceFile.length()))) {
				overQuota = true;
				if (info.success) {
					info.msg = "Ran out of space.";
					info.success = false;
				}
			}
			
			// move source file from pending folder to main files folder if processing was successful, otherwise remove source file
			if (info.success) {
				// move file from pending to main folder
				if (!FileHelper.moveToWebApp(sourceFile, getId())) {
					logger.error("An error occurred trying to move the source file with id "+getId()+" from the pending folder to the main folder.");
					errorMovingSourceFile = true;
				}
			}
			else {
				// delete file from pending
				// TODO: PUT THIS BACK!
	//			if (!sourceFile.delete()) {
	//				logger.warn("Failed to delete file with it "+getId()+" from pending directory as it failed processing. It might have failed proecssing because it was missing for some reason so this might be ok.");
	//			}
			}
		}
		
		if (!errorMovingSourceFile) {
			// update process_state in db and mark files as in_use
			Connection dbConnection = DbHelper.getMainDb().getConnection();
			logger.debug("Updating process_state in database...");
			try {
				logger.trace("Starting database transaction.");
				dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=?");
				s.setInt(1, getId());
				ResultSet r = s.executeQuery();
				if (!r.next()) {
					logger.error("Record could not be found in database for file with id "+getId()+".");
				}
				// check that ready_for_delete is still 0.
				else {
					if (r.getInt("ready_for_delete") == 1) {
						logger.debug("The file with id "+getId()+" has been processed but during this time it has been marked for deletion. Updating process_state anyway.");
					}
					
					
					if (info.success && info.fileIdsToMarkInUse.size() > 0) {
						
						String query = "UPDATE files SET in_use=1 WHERE id IN (";
						
						for (int i=0; i<info.fileIdsToMarkInUse.size(); i++) {
							if (i > 0) {
								query += ",";
							}
							query += "?";
						}
						query += ")";
						PreparedStatement s2 = dbConnection.prepareStatement(query);
						{
							int i=1;
							for(Integer id : info.fileIdsToMarkInUse) {
								s2.setInt(i++, id);
							}
						}
						if (s2.executeUpdate() != info.fileIdsToMarkInUse.size()) {
							logger.error("Error occurred setting in_use to 1. Processing will be marked as failing.");
							// rollback to make sure if some were marked as in_use they all get reverted back
							dbConnection.prepareStatement("ROLLBACK").executeUpdate();
							// start another transaction because rest of code in this try catch block is expecting transaction
							dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
							// causes processing to be marked as failure
							info.success = false;
						}
						s2.close();
					}
					
					// update process_state and set error message
					DbHelper.updateStatus(getId(), !info.success ? info.msg : "", null);
					PreparedStatement s2 = dbConnection.prepareStatement("UPDATE files SET process_state=? WHERE id=?");
					s2.setInt(1, info.success ? 1 : 2); // a value of 1 represents success, 2 represents failure
					s2.setInt(2, getId());
					if (s2.executeUpdate() != 1) {
						logger.trace("Rolling back database transaction.");
						dbConnection.prepareStatement("ROLLBACK").executeUpdate();
						logger.error("Error occurred updating process_state for file with id "+getId()+".");
					}
					else {
						logger.trace("Commiting database transaction.");
						dbConnection.prepareStatement("COMMIT").executeUpdate();
						logger.debug("Updated process_state in database.");
					}
					s2.close();
				}
				s.close();
			} catch (SQLException e) {
				try {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
				} catch (SQLException e1) {
					logger.debug("Transaction failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
				}
				logger.error("Error querying database in order to update file process_state for file with id "+getId()+" and mark files as in_use.");
			}
		}
		
		if (workingDirCreated) {
			logger.debug("Removing files working directory...");
			try {
				FileUtils.forceDelete(new java.io.File(fileWorkingDir));
				logger.debug("Removed files working directory.");
			} catch (IOException e) {
				logger.error("Error removing files working directory.");
			}
		}
		logger.info("Finished processing file with id "+getId()+" and name '"+getName()+"'.");
	}
}
