package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;

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

		@Override
		public void run() {
			synchronized(lock1) {
				Db db = DbHelper.getMainDb();
				Connection dbConnection = db.getConnection();
				
				// get list of file ids that should not be returned from queries because they are currently being processed
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
				
				// look for files to process
				try {
					
					logger.info("Polling for files that need processing...");
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
					
					PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE process_state=0 AND ready_for_delete=0"+fileTypeIdsWhere+fileIdsWhere+" ORDER BY updated_at DESC");
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
						File file = new File(r.getInt("id"), r.getString("filename"), r.getInt("size"), FileType.getFromId(r.getInt("file_type_id")));
						queue.add(r.getInt("id"));
						threadPool.execute(new Job(taskCompletionHandler, file));
						logger.info("Created and scheduled process job for file with id "+r.getInt("id")+".");
					}	
				} catch (SQLException e) {
					logger.error("SQLException when trying to query databases for files that need processing.");
				}
				logger.info("Finished polling for files that need processing.");
				
				logger.info("Polling for files pending deletion...");
				
				logger.info("Finished polling for files pending deletion.");
			}
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
