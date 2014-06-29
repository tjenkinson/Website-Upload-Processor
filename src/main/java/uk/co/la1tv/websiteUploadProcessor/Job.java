package uk.co.la1tv.websiteUploadProcessor;

public class Job implements Runnable {
	
	private CompletionHandlerI completionHandler;
	private File file;
	
	public Job(CompletionHandlerI completionHandler, File file) {
		this.completionHandler = completionHandler;
		this.file = file;
	}
	
	@Override
	public void run() {
		file.process();
		completionHandler.markCompletion(file.getId());
	}

}
