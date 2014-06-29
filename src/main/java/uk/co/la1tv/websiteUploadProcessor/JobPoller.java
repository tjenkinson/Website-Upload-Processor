package uk.co.la1tv.websiteUploadProcessor;

import java.util.LinkedHashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeBuilder;

/**
 * Scans the File table at set intervals looking for new files with file types that require processing.
 * If it finds a file that requires processing it adds that files id to the queue where it will be picked up by a worker.
 *
 */
public class JobPoller {
	// contains the ids of files that have been added to the threadPool for processing
	// ids are removed after the relevant process has completed.
	private LinkedHashSet<Integer> queue = new LinkedHashSet<>();
	private ExecutorService threadPool;
	private TaskCompletionHandler taskCompletionHandler;
	private Timer timer;
	
	public JobPoller() {
		Config config = Config.getInstance();
		threadPool = Executors.newFixedThreadPool(config.getInt("general.noThreads"));
		taskCompletionHandler = new TaskCompletionHandler();
		timer = new Timer(false);
		timer.schedule(new PollTask(), 0, config.getInt("general.pollInterval")*1000);
	}
	
	private class PollTask extends TimerTask {

		@Override
		public void run() {
			
			// lets say it found a file with id 5 that needs processing
			
			// create File obj
			File file = new File(5, "Name", 100, FileTypeBuilder.build(3));
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
