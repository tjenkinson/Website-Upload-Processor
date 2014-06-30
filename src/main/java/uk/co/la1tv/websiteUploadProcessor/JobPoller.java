package uk.co.la1tv.websiteUploadProcessor;

import java.util.LinkedHashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;

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
	
	public JobPoller() {
		logger.info("Loading Job Poller...");
		
		Config config = Config.getInstance();
		threadPool = Executors.newFixedThreadPool(config.getInt("general.noThreads"));
		taskCompletionHandler = new TaskCompletionHandler();
		timer = new Timer(false);
		timer.schedule(new PollTask(), 0, config.getInt("general.pollInterval")*1000);
		logger.info("Job poller loaded.");
	}
	
	private class PollTask extends TimerTask {

		@Override
		public synchronized void run() {
			
			logger.info("Polling...");
			
			// lets say it found a file with id 5 that needs processing
			
			int fileId = 5;
			
			// check this file id is not in the queue. It shouldn't be. If it is then there was something wrong with the db query!
			
			if (queue.contains(fileId)) {
				logger.error("A file id was retrieved from the database for processing but this id was already in the queue. This shouldn't happen because it should have been excluded in the database query.");
				//TODO: might end up being a continue;
				return;
			}
			
			// create File obj
			File file = new File(fileId, "Name", 100, FileType.getFromId(3));
			queue.add(5);
			threadPool.execute(new Job(taskCompletionHandler, file));
			
		}
		
	}
	
	private class TaskCompletionHandler implements CompletionHandlerI {
		
		/**
		 * Called from a Task after it's complete just before it finishes.
		 * @param file
		 */
		public void markCompletion(int id) {
			queue.remove(id);
			System.out.println(id);
		}
	}
	
}
