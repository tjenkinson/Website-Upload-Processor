package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.SQLException;



import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;

public class Job implements Runnable {
	
	private CompletionHandlerI completionHandler;
	private File file;
	private Connection dbConnection;
	
	public Job(CompletionHandlerI completionHandler, File file) {
		this.completionHandler = completionHandler;
		this.file = file;
		// create a new connection to the database for this job
		dbConnection = DbHelper.getMainDb().getConnection();
	}
	
	@Override
	public void run() {
		file.process(dbConnection);
		try {
			dbConnection.close();
		} catch (SQLException e) {
			throw(new RuntimeException("SQLException when trying to close database connection."));
		}
		completionHandler.markCompletion(file);
	}

}
