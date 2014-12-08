package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.SQLException;



import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;

public class Job implements Runnable {
	
	private final CompletionHandlerI completionHandler;
	private final File file;
	private final Connection dbConnection;
	private final Object initialHeartbeatManagerFileLockObj;
	
	public Job(CompletionHandlerI completionHandler, File file, Object initialHeartbeatManagerFileLockObj) {
		this.completionHandler = completionHandler;
		this.file = file;
		this.initialHeartbeatManagerFileLockObj = initialHeartbeatManagerFileLockObj;
		// create a new connection to the database for this job
		dbConnection = DbHelper.getMainDb().getConnection();
	}
	
	@Override
	public void run() {
		// switch the lock object on the file to the reference to the current thread
		HeartbeatManager.getInstance().switchLockObj(file, initialHeartbeatManagerFileLockObj);
		file.process(dbConnection);
		try {
			dbConnection.close();
		} catch (SQLException e) {
			throw(new RuntimeException("SQLException when trying to close database connection."));
		}
		completionHandler.markCompletion(file);
	}

}
