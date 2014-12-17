package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

/**
 * Scans the File table at set intervals looking for new files with file types that require processing.
 * If it finds a file that requires processing it adds that files id to the queue where it will be picked up by a worker.
 *
 */
public class JobPoller {
	
	private static Logger logger = Logger.getLogger(JobPoller.class);
	
	// contains the Files that have been added to the threadPool for processing
	// they are removed after the relevant process has completed.
	// when the processing has completed they won't be picked up again because the conditions for picking up a job will no longer be met in the database query
	private LinkedHashSet<File> filesInProgress = new LinkedHashSet<>();
	private ExecutorService threadPool;
	private TaskCompletionHandler taskCompletionHandler;
	private Timer timer;
	private Config config;
	private HeartbeatManager heartbeatManager;
	
	private Object lock1 = new Object();
	
	public JobPoller() {
		logger.info("Loading Job Poller...");
		config = Config.getInstance();
		threadPool = Executors.newFixedThreadPool(config.getInt("general.noThreads"));
		taskCompletionHandler = new TaskCompletionHandler();
		heartbeatManager = HeartbeatManager.getInstance();
		timer = new Timer(false);
		timer.schedule(new PollTask(), 0, config.getInt("general.pollInterval")*1000);
		logger.info("Job poller loaded.");
	}
	
	private class PollTask extends TimerTask {
	
		
		@Override
		public void run() {
			synchronized(lock1) {
				final Connection dbConnection = DbHelper.getMainDb().getConnection();
				if (dbConnection == null) {
					logger.warn("Can't poll for files to be processed or updated at the moment as can't connect to database.");
					return;
				}
				handleFilesForReprocessing(dbConnection);
				deleteFiles(dbConnection);
				processFiles(dbConnection);
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		// look for files to process kick of jobs to process them
		private void processFiles(Connection dbConnection) {
			logger.info("Polling for files that need processing...");
			try {
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE ready_for_processing=1 AND process_state=0 AND ready_for_delete=0 AND (session_id IS NOT NULL OR in_use=1) AND (heartbeat IS NULL OR heartbeat<?)"+getFileTypeIdsWhereString("file_type_id")+" ORDER BY updated_at DESC");
				int i = 1;
				s.setTimestamp(i++, heartbeatManager.getProcessingFilesTimestamp());
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				
				ResultSet r = s.executeQuery();
				
				while(r.next()) {
					logger.info("Looking at file with id "+r.getInt("id")+".");
					
					// create File obj
					File file = DbHelper.buildFileFromResult(r);
					
					if (filesInProgress.size() >= config.getInt("general.noThreads")) {
						logger.info("File with id "+file.getId()+" will not be picked up at the moment as there are no free threads available.");
						continue;
					}
					
					Object heartbeatManagerFileLockObj = new Object();
					
					Job job = null;
					try {
						if (!heartbeatManager.registerFile(file, false, heartbeatManagerFileLockObj)) {
							logger.info("File with id "+file.getId()+" will not be processed because it's heartbeat was updated somewhere else.");
							continue;
						}
						
						DbHelper.updateStatus(dbConnection, file.getId(), "Added to process queue.", null);
						
						filesInProgress.add(file);
						job = new Job(taskCompletionHandler, file, heartbeatManagerFileLockObj);
					}
					catch(Exception e) {
						// an exception occurred so unregister the file and then rethrow the exception.
						heartbeatManager.unRegisterFile(file);
						filesInProgress.remove(file);
						throw(e);
					}
					threadPool.execute(job);
					logger.info("Created and scheduled process job for file with id "+r.getInt("id")+".");
				}
				s.close();
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need processing.");
				e.printStackTrace();
			}
			logger.info("Finished polling for files that need processing.");
		}
		
		// first look for files with the reprocess flag set and a process_state of 1, and set the process_state to 3 which means prepare for reprocessing, and set the reprocess flag back to 0
		
		// find files with a process_state of 3. if this is the case attempt to delete all child files and set the process_state back to 0 so it will be picked up again, or leave it set to 3 if some of this fails
		private void handleFilesForReprocessing(Connection dbConnection) {
			logger.info("Looking for files that are set to be reprocessed.");
			try {
				// first look for files that have the reprocess flag set and process_state as 1 and update their process_state and reset the flag
				PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET process_state=3, reprocess=0 WHERE reprocess=1 AND process_state=1"+getFileTypeIdsWhereString("file_type_id"));
				int i = 1;
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				s.executeUpdate();
			} catch (SQLException e) {
				logger.error("SQLException when trying to update process_state for file files that have reprocess flag set.");
				e.printStackTrace();
			}
			
			try {
				// go through all files with a process_state of 3 an attempt to delete their child files. if this is successful then set the process_state back to 0 so it can be processed again
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE ((heartbeat IS NULL OR heartbeat<?) AND process_state=3)"+getFileTypeIdsWhereString("file_type_id"));
				int i = 1;
				s.setTimestamp(i++, heartbeatManager.getProcessingFilesTimestamp());
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				ResultSet r = s.executeQuery();
				
				while(r.next()) {
					File file = DbHelper.buildFileFromResult(r);
					logger.info("Found file with id "+file.getId()+" that wants reprocessing.");
					if (!HeartbeatManager.getInstance().registerFile(file)) {
						logger.info("File with id "+file.getId()+" cannot be reprocessed now as it is being used somewhere else.");
						continue;
					}
					try {
						logger.info("Attempting to remove any child files it has.");
						if (!removeChildFilesAndRecords(dbConnection, file, false)) {
							logger.warn("Some child files could not be removed for some reason. Not resetting process_state to 0.");
						}
						else {
							// set process_state to 0 so that it will be picked up for processing again
							s = dbConnection.prepareStatement("UPDATE files SET process_state=0 WHERE id=?");
							s.setInt(1, file.getId());
							if (s.executeUpdate() != 1) {
								logger.error("There was an error setting the process_state for file with id "+file.getId()+".");
							}
							
							DbHelper.updateStatus(dbConnection, file.getId(), "Waiting to be reprocessed.", null);
							logger.info("File with id "+file.getId()+" is now ready for reprocessing.");
						}
						HeartbeatManager.getInstance().unRegisterFile(file);
					}
					catch(Exception e) {
						// an exception occurred so unregister the file and then rethrow the exception.
						HeartbeatManager.getInstance().unRegisterFile(file);
						throw(e);
					}
				}
				s.close();
			} catch (SQLException e) {
				logger.error("SQLException when trying setup a file for reprocessing.");
				e.printStackTrace();
			}
			
			logger.info("Finished looking for files that are set to be reprocessed.");
		}
		
		// look for files that are pending deletion, or temporary and no longer belong to a session, and delete them.
		private void deleteFiles(Connection dbConnection) {
			
			logger.info("Polling for files pending deletion...");
			try {
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE ready_for_processing=1 AND ((heartbeat IS NULL OR heartbeat<?) AND (ready_for_delete=1 OR (in_use=0 AND session_id IS NULL)))"+getFileTypeIdsWhereString("file_type_id"));
				int i = 1;
				s.setTimestamp(i++, heartbeatManager.getProcessingFilesTimestamp());
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				ResultSet r = s.executeQuery();
				
				while(r.next()) {
					File file = DbHelper.buildFileFromResult(r);
					logger.info("Attempting to remove file with id "+file.getId()+" and any child files it has.");
					if (!removeChildFilesAndRecords(dbConnection, file, true)) {
						logger.warn("Some files could not be removed for some reason and the file record for this (id "+file.getId()+") still exists.");
					}
				}
				s.close();
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need deleting.");
				e.printStackTrace();
			}
			logger.info("Finished polling for files pending deletion.");
		}
			
		private String getFileTypeIdsWhereString(String col) {
			// the ids of the file types that we know about.
			// we are not interested in any other file type ids that aren't listed
			// all file types in laravel should be duplicated here
			String fileTypeIdsWhere = "";
			if (FileType.values().length > 0) {
				fileTypeIdsWhere = " AND "+col+" IN (";
				for (int i=0; i<FileType.values().length; i++) {
					if (i > 0) {
						fileTypeIdsWhere += ",";
					}
					fileTypeIdsWhere += "?";
				}
				fileTypeIdsWhere += ")";
			}
			return fileTypeIdsWhere;
		}
		
		/**
		 * Removes all child files and optionally the source file.
		 * If any child files have child files they will also be delete recursively.
		 * The source file and record will never be removed if one or more of the child files fails to be removed.
		 * The source and child files will be registered and unregistered with the heartbeat manager.
		 * @param dbConnection
		 * @param sourceFile
		 * @param removeSourceFile
		 * @return true if all files and records were removed successfully or false if one or more failed.
		 */
		private boolean removeChildFilesAndRecords(Connection dbConnection, File sourceFile, boolean removeSourceFile) {
			
			if (!heartbeatManager.registerFile(sourceFile)) {
				logger.info("File with id "+sourceFile.getId()+" and any of its child files will not be deleted right now because it's heartbeat was updated somewhere else.");
				return false;
			}
			
			boolean allFilesDeletedSuccesfully = true;
			
			try {
				boolean sourceFileMarkedForDeletion = false;
				
				// delete actual files
				
				{
					if (removeSourceFile) {
						// set the ready_for_delete flag to 1 so that if some of the child files fail to be deleted, or this file, apps can still know that this is pending deletion and may be missing some child files, and should not be used
						// if the ready_for_delete flag is set then apps should treat this as though the file has already been deleted
						try {
							dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
							PreparedStatement s = dbConnection.prepareStatement("SELECT ready_for_delete FROM files WHERE id=? FOR UPDATE");
							s.setInt(1, sourceFile.getId());
							s.executeQuery();
							ResultSet r = s.getResultSet();
							if (r.next()) {
								// record exists and now have lock
								if (!r.getBoolean("ready_for_delete")) {
									// this is not marked for deletion. Mark it
									s = dbConnection.prepareStatement("UPDATE files SET ready_for_delete=1 WHERE id=?");
									s.setInt(1, sourceFile.getId());
									if (s.executeUpdate() == 1) {
										sourceFileMarkedForDeletion = true;
									}
									else {
										logger.error("There was an error trying to update ready_for_delete for source file with id "+sourceFile.getId()+".");
									}
								}
								else {
									sourceFileMarkedForDeletion = true;
								}
							}
							else {
								logger.error("Could not mark source file with id "+sourceFile.getId()+" for deletion because it couldn't be found!");
							}
							dbConnection.prepareStatement("COMMIT").executeUpdate();
						}
						catch(SQLException e) {
							logger.error("An SQLException occurred whilst trying to mark file record with id "+sourceFile.getId()+" for deletion.");
						}
					}
				}
				
				if (!removeSourceFile || sourceFileMarkedForDeletion) {
				
					{
						try {
							PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE (heartbeat IS NULL OR heartbeat<?) AND source_file_id=?");
							s.setTimestamp(1, heartbeatManager.getProcessingFilesTimestamp());
							s.setInt(2, sourceFile.getId());
							ResultSet r = s.executeQuery();
							
							while(r.next()) {
								// create File obj
								File file = DbHelper.buildFileFromResult(r);
								// remove this files child files, then the file itself (if deleting all the child files is successful)
								if (!removeChildFilesAndRecords(dbConnection, file, true)) {
									// returns false if one or more files out of the current one or its descendants could not be removed.
									logger.error("An error occurred when trying to remove file with id "+file.getId()+" and any child files it has.");
									allFilesDeletedSuccesfully = false;
								}
							}
							s.close();
						} catch (SQLException e) {
							e.printStackTrace();
							logger.error("SQLException when trying to query databases for files that need deleting.");
							allFilesDeletedSuccesfully = false;
						}
					}
					
					{
						if (removeSourceFile && allFilesDeletedSuccesfully) {
							//all child files removed successfully. now remove source
							allFilesDeletedSuccesfully = false;
							
							String sourceFilePath = FileHelper.getSourceFilePath(sourceFile.getId());
							
							// delete file
							boolean fileDeleted = false;
							try {
								if (Files.exists(Paths.get(sourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
									FileUtils.forceDelete(new java.io.File(sourceFilePath));
									if (!Files.exists(Paths.get(sourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
										// file no longer exists
										logger.debug("Deleted file with id "+sourceFile.getId()+".");
										fileDeleted = true;
									}
								}
								else {
									logger.debug("File with id "+sourceFile.getId()+" could not be deleted because it doesn't exist! Just removing record. This is possible if a file failed to copy accross after it's record was created.");
									fileDeleted = true;
								}
							} catch (IOException e) {
								logger.error("Error deleting file with id "+sourceFile.getId()+".");
							}
							
							if (fileDeleted) {
								try {
									PreparedStatement s = dbConnection.prepareStatement("DELETE FROM files WHERE id=?");
									s.setInt(1, sourceFile.getId());
									if (s.executeUpdate() != 1) {
										logger.error("Error occurred whilst deleting file record with id "+sourceFile.getId()+".");
									}
									else {
										// the file and record have now been removed
										allFilesDeletedSuccesfully = true;
									}
								} catch (SQLException e) {
									logger.error("SQLException when trying to query databases for files that need deleting.");
								}
							}
						}
					}
				}
				heartbeatManager.unRegisterFile(sourceFile);
			}
			catch(Exception e) {
				// an exception occurred so unregister the file and then rethrow the exception.
				heartbeatManager.unRegisterFile(sourceFile);
				throw(e);
			}
			return allFilesDeletedSuccesfully;
		}
	}
	
	private class TaskCompletionHandler implements CompletionHandlerI {
		
		/**
		 * Called from a Task after it's completed just before it finishes.
		 * @param file
		 */
		public void markCompletion(File file) {
			synchronized(lock1) {
				heartbeatManager.unRegisterFile(file);
				filesInProgress.remove(file);
			}
		}
	}
	
}
