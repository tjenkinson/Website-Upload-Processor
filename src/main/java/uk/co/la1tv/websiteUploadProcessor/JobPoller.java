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
		
		private final Connection dbConnection;
		
		public PollTask() {
			dbConnection = DbHelper.getMainDb().getConnection();
		}
		
		@Override
		public void run() {
			synchronized(lock1) {
				
				deleteFiles();
				processFiles();
			}
		}
		
		// look for files to process kick of jobs to process them
		private void processFiles() {
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
					
					if (!heartbeatManager.registerFile(file)) {
						logger.info("File with id "+file.getId()+" will not be processed because it's heartbeat was updated somewhere else.");
						continue;
					}
					
					DbHelper.updateStatus(dbConnection, file.getId(), "Added to process queue.", null);
					filesInProgress.add(file);
					threadPool.execute(new Job(taskCompletionHandler, file));
					logger.info("Created and scheduled process job for file with id "+r.getInt("id")+".");
				}
				s.close();
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need processing.");
				e.printStackTrace();
			}
			logger.info("Finished polling for files that need processing.");
		}
		
		// look for files that are pending deletion, or temporary and no longer belong to a session, and delete them.
		private void deleteFiles() {
			
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
					if (!removeChildFilesAndRecords(file, true)) {
						logger.warn("Some files could not be removed for some reason and the file record for this (id "+file.getId()+") still exists.");
					}
				}
				s.close();
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need deleting.");
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
		private boolean removeChildFilesAndRecords(File sourceFile, boolean removeSourceFile) {
			
			if (!heartbeatManager.registerFile(sourceFile)) {
				logger.info("File with id "+sourceFile.getId()+" and any of its child files will not be deleted right now because it's heartbeat was updated somewhere else.");
				return false;
			}
			
			boolean sourceFileMarkedForDeletion = false;
			boolean allFilesDeletedSuccesfully = true;
			
			// delete actual files
			
			{
				if (removeSourceFile) {
					// set the ready_for_delete flag to 1 so that if some of the child files fail to be deleted apps can still know that this is pending deletion and may be missing some child files
					// if the ready_for_delete flag is set then apps should treat this as though the file has already been deleted
					
					try {
						PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET ready_for_delete=1 WHERE id=?");
						s.setInt(1, sourceFile.getId());
						s.executeUpdate();
						sourceFileMarkedForDeletion = true;
					}
					catch(SQLException e) {
						logger.error("An SQLException occurred whilst trying to mark file record with id "+sourceFile.getId()+" for deletion.");
					}
				}
			}
			
			if (removeSourceFile && sourceFileMarkedForDeletion) {
			
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
							if (!removeChildFilesAndRecords(file, true)) {
								// one or more files out of the current one or its descendants could not be removed.
								allFilesDeletedSuccesfully = false;
							}
						}
						s.close();
					} catch (SQLException e) {
						logger.error("SQLException when trying to query databases for files that need deleting.");
						allFilesDeletedSuccesfully = false;
					}
				}
				
				{
					if (removeSourceFile && allFilesDeletedSuccesfully) {
						//all child files removed successfully. now remove source
						allFilesDeletedSuccesfully = false;
						
						String sourceFilePath = FileHelper.getSourceFilePath(sourceFile.getId());
						String sourcePendingFilePath = FileHelper.getSourcePendingFilePath(sourceFile.getId());
						// presume if the file can't be found in the main folder it's in pending instead.
						String actualSourceFilePath = Files.exists(Paths.get(sourceFilePath), LinkOption.NOFOLLOW_LINKS) ? sourceFilePath : sourcePendingFilePath;
						
						// delete file
						boolean fileDeleted = false;
						try {
							if (Files.exists(Paths.get(actualSourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
								FileUtils.forceDelete(new java.io.File(actualSourceFilePath));
								if (!Files.exists(Paths.get(actualSourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
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
