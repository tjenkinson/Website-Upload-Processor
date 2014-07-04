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
	
	// contains the ids of files that have been added to the threadPool for processing
	// ids are removed after the relevant process has completed.
	// when the processing has completed they won't be picked up again because the conditions for picking up a job will no longer be met in the database query
	private LinkedHashSet<Integer> queue = new LinkedHashSet<>();
	private ExecutorService threadPool;
	private TaskCompletionHandler taskCompletionHandler;
	private Timer timer;
	private Config config;
	
	private Object lock1 = new Object();
	
	public JobPoller() {
		logger.info("Loading Job Poller...");	
		config = Config.getInstance();
		threadPool = Executors.newFixedThreadPool(config.getInt("general.noThreads"));
		taskCompletionHandler = new TaskCompletionHandler();
		timer = new Timer(false);
		timer.schedule(new PollTask(), 0, config.getInt("general.pollInterval")*1000);
		logger.info("Job poller loaded.");
	}
	
	private class PollTask extends TimerTask {
		
		private Db db = DbHelper.getMainDb();
		
		@Override
		public void run() {
			synchronized(lock1) {
				processFiles();
				deleteFiles();
			}
		}
		
		// look for files to process kick of jobs to process them
		private void processFiles() {
			logger.info("Polling for files that need processing...");
			try {
				Connection dbConnection = db.getConnection();
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE process_state=0 AND ready_for_delete=0"+getFileTypeIdsWhereString()+getFileIdsWhereString()+" ORDER BY updated_at DESC");
				int i = 1;
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				for (Integer a : queue) {
					s.setInt(i++, a);
				}
				ResultSet r = s.executeQuery();
				
				while(r.next()) {
					logger.info("Looking at file with id "+r.getInt("id")+".");
					
					// check this file id is not in the queue. It shouldn't be. If it is then there was something wrong with the db query!
					if (queue.contains(r.getInt("id"))) {
						logger.error("A file id was retrieved from the database for processing but this id was already in the queue. This shouldn't happen because it should have been excluded in the database query.");
						continue;
					}
					
					// create File obj
					File file = DbHelper.buildFileFromResult(r);
					queue.add(r.getInt("id"));
					threadPool.execute(new Job(taskCompletionHandler, file));
					logger.info("Created and scheduled process job for file with id "+r.getInt("id")+".");
				}	
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need processing.");
			}
			logger.info("Finished polling for files that need processing.");
		}
		
		// look for files that are pending deletion, or temporary and no longer belong to a session, and delete them.
		private void deleteFiles() {
			
			// TODO: need to check if has source files and remove those as well
			
			logger.info("Polling for files pending deletion...");
			try {
				Connection dbConnection = db.getConnection();
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE (ready_for_delete=1 OR (in_use=0 AND session_id IS NULL))"+getFileTypeIdsWhereString()+getFileIdsWhereString()+" ORDER BY updated_at DESC");
				int i = 1;
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				for (Integer a : queue) {
					s.setInt(i++, a);
				}
				ResultSet r = s.executeQuery();
				while(r.next()) {
					logger.info("Found file with id "+r.getInt("id")+" that is pending deletion.");
					
					// create File obj
					File file = DbHelper.buildFileFromResult(r);
					String sourceFilePath = FileHelper.getSourceFilePath(r.getInt("id"));
					
					// delete file
					try {
						if (Files.exists(Paths.get(sourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
							FileUtils.forceDelete(new java.io.File(sourceFilePath));
						}
						else {
							logger.debug("File with id "+file.getId()+" which is marked for deletion could not be deleted because it doesn't exist!");
						}
						// remove record from db
						PreparedStatement stmnt = dbConnection.prepareStatement("DELETE FROM files WHERE id=?");
						stmnt.setInt(1, file.getId());
						if (stmnt.executeUpdate() != 1) {
							logger.error("Error when deleteing record from database for file with id "+file.getId()+".");
						}
						else {
							logger.info("File with id "+r.getInt("id")+" deleted and removed from database.");
						}
						
					} catch (IOException e) {
						logger.error("Error deleting file with id "+file.getId()+" which was pending deletion.");
					}
				}
				
			} catch (SQLException e) {
				logger.error("SQLException when trying to query databases for files that need deleting.");
			}
			logger.info("Finished polling for files pending deletion.");
		}
		
		// get string with placeholders for file ids that should not be returned from queries because they are currently being processed
		private String getFileIdsWhereString() {
			String fileIdsWhere = "";
			if (queue.size() > 0) {
				fileIdsWhere = " AND id NOT IN (";
				for (int i=0; i<queue.size(); i++) {
					if (i > 0) {
						fileIdsWhere += ",";
					}
					fileIdsWhere += "?";
				}
				fileIdsWhere += ")";
			}
			return fileIdsWhere;
		}
		
		private String getFileTypeIdsWhereString() {
			// the ids of the file types that we know about.
			// we are not interested in any other file type ids that aren't listed
			// all file types in laravel should be duplicated here
			String fileTypeIdsWhere = "";
			if (FileType.values().length > 0) {
				fileTypeIdsWhere = " AND file_type_id IN (";
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
	}
	
	private class TaskCompletionHandler implements CompletionHandlerI {
		
		/**
		 * Called from a Task after it's complete just before it finishes.
		 * @param file
		 */
		public void markCompletion(int id) {
			synchronized(lock1) {
				queue.remove(id);
			}
		}
	}
	
}
