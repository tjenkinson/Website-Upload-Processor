package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeAbstract;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeProcessReturnInfo;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

public class File {
	
	private static Logger logger = Logger.getLogger(File.class);
	
	private int id;
	private String name;
	private long size;
	private FileTypeAbstract type;
	
	public File(int id, String name, long size, FileTypeAbstract type) {
	
		this.id = id;
		this.name = name;
		this.size = size;
		this.type = type;
		String namePart = name != null ? " and name '"+name+"'" : "";
		logger.debug("Created File object for file of type '"+type.getClass().getSimpleName()+"' with id "+id+namePart+".");
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
	
	public long getSize() {
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
	
	public void process(Connection dbConnection) {
		
		Config config = Config.getInstance();
		// used to signify error if problem copying file from pending files locatioon to server (if working with copy) or if there was a problem moving the source fie from the pending directory to the main files directory.
		boolean errorCopyingSourceFile = false;
		// true if the webapp folder is over quota meaning the job should be failed.
		boolean overQuota = FileHelper.isOverQuota();
		boolean workingDirCreated = false;
		FileTypeProcessReturnInfo info = null;
		
		logger.info("Started processing file with id "+getId()+" and name '"+getName()+"'.");
		DbHelper.updateStatus(dbConnection, getId(), "Started processing.", null);
		
		{
			// update the process start time in the database
			try {
				PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET process_start_time=? WHERE id=?");
				s.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
				s.setInt(2, getId());
				if (s.executeUpdate() != 1) {
					logger.error("There was an error updating the process_start_time for file with id "+getId()+".");
				}
			} catch (SQLException e) {
				logger.error("SQLException when trying to update process_start_time.");
				e.printStackTrace();
			}
		}
		
		boolean workingWithCopy = config.getBoolean("general.workWithCopy");
		String fileWorkingDir = FileHelper.getFileWorkingDir(getId());
		String sourceFilePath = FileHelper.getSourceFilePath(getId());
		// the path to file that will be processed. (It might be copied to the working dir so this will be different to sourceFilePath)
		String destinationSourceFilePath = sourceFilePath;
		
		if (!overQuota) {
			logger.debug("Creating folder for file in working directory...");
			
			try {
				FileUtils.forceMkdir(new java.io.File(fileWorkingDir));
			} catch (IOException e) {
				throw(new RuntimeException("Error creating folder for file to process."));
			}
			workingDirCreated = true; // Purposefully put this outside try block in case it it is partly created maybe, in which case it would still be good to attempt to delete it later.
			logger.debug("Created folder for file in working directory.");
			if (workingWithCopy) {
				try {
					destinationSourceFilePath = FileHelper.format(fileWorkingDir+"/source");
					logger.debug("Copying file with id "+getId()+" to working directory...");
					FileUtils.copyFile(new java.io.File(sourceFilePath), new java.io.File(destinationSourceFilePath));
					logger.debug("Copied file with id "+getId()+" to working directory.");
				} catch (IOException e) {
					errorCopyingSourceFile = true;
					logger.error("Error copying file with id "+getId()+" from web app files location to working directory.");
					e.printStackTrace();
				}
			}
		}
		
		if (!errorCopyingSourceFile && !overQuota) {
			try {
				info = type.process(dbConnection, new java.io.File(destinationSourceFilePath), new java.io.File(fileWorkingDir), this, workingWithCopy);
			}
			catch (Exception e) {
				e.printStackTrace();
				logger.error("An exception was thrown whilst tryingn to process file with id "+getId()+".");
				info = new FileTypeProcessReturnInfo();
				info.msg = "An unexpected error occurred.";
			}
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
		
		if (!errorCopyingSourceFile) {
			
			java.io.File sourceFile = new java.io.File(sourceFilePath);
			
			// check if would be over quote now when source file moved across
			if (FileHelper.isOverQuota(BigInteger.valueOf(sourceFile.length()))) {
				overQuota = true;
				if (info.success) {
					info.msg = "Ran out of space.";
					info.success = false;
				}
			}
			
			if (!info.success) {
				// the processing failed so remove the source file. no point keeping it if an error occurred,
				// unless this error has occurred during reprocessing in which case don't delete it so reprocessing can be done again
				try {
					
					PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=?");
					s.setInt(1, getId());
					ResultSet r = s.executeQuery();
					if (!r.next()) {
						logger.error("Error querying database to determine if file with id "+getId()+" has already been processed successfully. Could not find file record.");
					}
					else if (!r.getBoolean("has_processed_successfully")) {
						// this is the first attempt at processing the file and it failed.
						// remove the source file
						if (!sourceFile.delete()) {
							logger.warn("Failed to delete file with id "+getId()+" as it failed processing. It might have failed proecssing because it was missing for some reason so this might be ok.");
						}
						else {
							logger.debug("Deleted source file with id "+getId()+" as it failed processing.");
						}
					}
					
					s.close();
				} catch (SQLException e) {
					logger.error("Error querying database to determine if file with id "+getId()+" has already been processed successfully.");
				}
			}
		}
		
		if (!errorCopyingSourceFile) {
			// update process_state in db and mark files as in_use
			logger.debug("Updating process_state in database...");
			try {
				logger.trace("Starting database transaction.");
				dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
				// get an exclusive lock on the source file then check that we still have the file registered with the heartbeat manager
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE id=? FOR UPDATE");
				s.setInt(1, getId());
				ResultSet r = s.executeQuery();
				
				boolean lostHeartbeatRegistration = false;
				if (!r.next()) {
					logger.error("Record could not be found in database for file with id "+getId()+".");
					lostHeartbeatRegistration = true; // if the file record is gone presume lost registration, as if this is not the case it will happen shortly
				}
				else {
					// check the file is still registered with the heartbeat manager.
					// could have lost the registration if there were sqlexceptions or other database issues. Unlikely but possible.
					// if the registration has been lost another server could currently also be processing this file. As long as we don't update the process state or mark the files as in use everything is still stable.
					lostHeartbeatRegistration = !HeartbeatManager.getInstance().isFileRegistered(this);
				}
				s.close();
					
				if (!lostHeartbeatRegistration && info.success && info.getNewFiles().size() > 0) {
					
					String query = "UPDATE files SET in_use=1 WHERE id IN (";
					
					for (int i=0; i<info.getNewFiles().size(); i++) {
						if (i > 0) {
							query += ",";
						}
						query += "?";
					}
					query += ")";
					PreparedStatement s2 = dbConnection.prepareStatement(query);
					{
						int i=1;
						for(File file : info.getNewFiles()) {
							s2.setInt(i++, file.getId());
						}
					}
					if (s2.executeUpdate() != info.getNewFiles().size()) {
						logger.error("Error occurred setting in_use to 1. Processing will be marked as failing.");
						// causes processing to be marked as failure
						info.success = false;
						// rollback to make sure if some were marked as in_use they all get reverted back
						dbConnection.prepareStatement("ROLLBACK").executeUpdate();
						// start another transaction because rest of code in this try catch block is expecting transaction
						dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
						
						// get an exclusive lock on the source file then check again that we still have the file registered with the heartbeat manager
						PreparedStatement s3 = dbConnection.prepareStatement("SELECT * FROM files WHERE id=? FOR UPDATE");
						s3.setInt(1, getId());
						ResultSet r2 = s3.executeQuery();
						if (!r2.next()) {
							logger.error("Record could no longer be found in database for file with id "+getId()+"."); // therefore we must have now lost the registration with the heartbeat manager for some reason, or we are about to next time it tries to update the timestamp and realises the record has gone
							lostHeartbeatRegistration = true;
						}
						else {
							lostHeartbeatRegistration = !HeartbeatManager.getInstance().isFileRegistered(this);
						}
						s3.close();
						
					}
					s2.close();
				}
				
				if (!lostHeartbeatRegistration) {
					// update process_state and set error message
					DbHelper.updateStatus(dbConnection, getId(), !info.success ? info.msg : "", null);
					PreparedStatement s2 = dbConnection.prepareStatement("UPDATE files SET process_state=? WHERE id=?");
					s2.setInt(1, info.success ? 1 : 2); // a value of 1 represents success, 2 represents failure
					s2.setInt(2, getId());
					boolean success = s2.executeUpdate() == 1;
					if (success) {
						if (info.success) {
							PreparedStatement s3 = dbConnection.prepareStatement("UPDATE files SET has_processed_successfully=1 WHERE id=?");
							s3.setInt(1, getId());
							s3.executeUpdate(); // this may return 0 because has_processed_successfully might already be 1 if this is a reprocessing
							s3.close();
						}
					}
					if (!success) {
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
				else {
					logger.trace("Rolling back database transaction.");
					// nothing should have changed, but might as well rollback instead of commit to be sure
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					logger.error("Heartbeat registration was lost for for file with id "+getId()+" for some reason, and therefore files were not marked as in_use and the process state was not updated.");
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
		
		// now unregister all the new files that have been created from the heartbeat manager.
		// they should have been registered at the time they were created
		for(File file : info.getNewFiles()) {
			HeartbeatManager.getInstance().unRegisterFile(file);
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
		
		{
			// update the process end time in the database
			try {
				PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET process_end_time=? WHERE id=?");
				s.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
				s.setInt(2, getId());
				if (s.executeUpdate() != 1) {
					logger.error("There was an error updating the process_end_time for file with id "+getId()+".");
				}
			} catch (SQLException e) {
				logger.error("SQLException when trying to update process_end_time.");
				e.printStackTrace();
			}
		}
		
		logger.info("Finished processing file with id "+getId()+" and name '"+getName()+"'.");
	}
}
