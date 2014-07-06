package uk.co.la1tv.websiteUploadProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
				deleteFiles();
				processFiles();
			}
		}
		
		// look for files to process kick of jobs to process them
		private void processFiles() {
			logger.info("Polling for files that need processing...");
			try {
				Connection dbConnection = db.getConnection();
				PreparedStatement s = dbConnection.prepareStatement("SELECT * FROM files WHERE process_state=0 AND ready_for_delete=0"+getFileTypeIdsWhereString("file_type_id")+getFileIdsWhereString("id")+" ORDER BY updated_at DESC");
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
			
			logger.info("Polling for files pending deletion...");
			try {
				Connection dbConnection = db.getConnection();
				// the ordering makes sure that records with source_file_id set appear first. As these must be deleted first
				PreparedStatement s = dbConnection.prepareStatement("SELECT o.id, o.filename, o.size, o.file_type_id, i.id, i.filename, i.size, i.file_type_id FROM files AS o LEFT JOIN files AS i ON i.source_file_id = o.id WHERE (o.ready_for_delete=1 OR (o.in_use=0 AND o.session_id IS NULL))"+getFileTypeIdsWhereString("o.file_type_id")+getFileIdsWhereString("o.id")+" ORDER BY (CASE WHEN i.id IS NULL THEN 1 ELSE 0 END) ASC");
				int i = 1;
				for (FileType a : FileType.values()) {
					s.setInt(i++, a.getObj().getId());
				}
				for (Integer a : queue) {
					s.setInt(i++, a);
				}
				ResultSet r = s.executeQuery();
				ArrayList<File> recordsToDelete = new ArrayList<File>();
				while(r.next()) {
					if (r.getInt("i.id") == 0) { // 0 means NULL
						logger.info("Found file with id "+r.getInt("o.id")+" that is pending deletion.");
					}
					else {
						logger.info("Found file with id "+r.getInt("o.id")+" with parent "+r.getInt("i.id")+" that is pending deletion. Both files will be deleted.");
					}
					
					File f;
					if (r.getInt("i.id") != 0) { // 0 returned if NULL
						f = new File(r.getInt("i.id"), r.getString("i.filename"), r.getInt("i.size"), FileType.getFromId(r.getInt("i.file_type_id")));
						if (!recordsToDelete.contains(f)) {
							recordsToDelete.add(0, f);
						}
					}
					f = new File(r.getInt("o.id"), r.getString("o.filename"), r.getInt("o.size"), FileType.getFromId(r.getInt("o.file_type_id")));		
					if (!recordsToDelete.contains(f)) {
						recordsToDelete.add(f);
					}
				}
				
				// delete actual files
				for(File file : recordsToDelete) {
					String sourceFilePath = FileHelper.getSourceFilePath(file.getId());	
					// delete file
					try {
						if (Files.exists(Paths.get(sourceFilePath), LinkOption.NOFOLLOW_LINKS)) {
							FileUtils.forceDelete(new java.io.File(sourceFilePath));
							logger.debug("Deleted file with id "+file.getId()+".");
						}
						else {
							logger.debug("File with id "+file.getId()+" which is marked for deletion could not be deleted because it doesn't exist! Just removing record. This is possible if a file failed to copy accross after it's record was created.");
						}
					} catch (IOException e) {
						logger.error("Error deleting file with id "+file.getId()+" which was pending deletion.");
					}
				}
				
				// now remove records from database
				// TODO: probably a more efficient way of doing this
				// tried doing WHERE IN with the ids in order but still was getting integrity constraint error. Maybe it doesn't necessarily process them in order specified in the IN section?
				if (recordsToDelete.size() > 0) {
					for (File f : recordsToDelete) {
						s = dbConnection.prepareStatement("DELETE FROM files WHERE id=?");
						s.setInt(1, f.getId());
						if (s.executeUpdate() != 1) {
							throw(new RuntimeException("Error occurred whilst deleting file record with id "+f.getId()+"."));
						}
					}
				}
				
			} catch (SQLException e) {
				// TODO: this is occurring on server
				logger.error("SQLException when trying to query databases for files that need deleting.");
			}
			logger.info("Finished polling for files pending deletion.");
		}
		
		// get string with placeholders for file ids that should not be returned from queries because they are currently being processed
		private String getFileIdsWhereString(String col) {
			String fileIdsWhere = "";
			if (queue.size() > 0) {
				fileIdsWhere = " AND "+col+" NOT IN (";
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
