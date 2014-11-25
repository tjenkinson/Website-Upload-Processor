package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;

// TODO: make this singleton
public class HeartbeatManager {
	
	private static Logger logger = Logger.getLogger(HeartbeatManager.class);
	
	private static HeartbeatManager instance = null;
	
	private final Timer timer;
	private final Config config;
	private final HashSet<FileAndCounter> files;
	private final long updateInterval;
	private final Object lock1 = new Object();
	
	private HeartbeatManager() {
		logger.info("Loading HeartbeatManager...");
		config = Config.getInstance();
		timer = new Timer(false);
		files = new HashSet<FileAndCounter>();
		updateInterval = config.getInt("general.heartbeatInterval")*1000;
		timer.schedule(new Task(), 0, updateInterval);
		logger.info("Loaded HeartbeatManager.");
	}
	
	public static synchronized HeartbeatManager getInstance() {
		if (instance == null) {
			instance = new HeartbeatManager();
		}
		return instance;
	}
	
	// register a file that is processing
	// returns true if the file was successfully registered.
	// could be false if the same file is registered at the same time from different servers. Only one will win.
	// a file can be registered several times (as long future registrations are from the same thread) and must be unregistered the same amount of times.
	public boolean registerFile(File file) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					// file already registered
					if (fileAndCounter.wasCreatedByCurrentThread()) {
						// increment the counter instead.
						fileAndCounter.register();
						return true;
					}
					else {
						// a different thread registered this file and is still using it so deny access.
						logger.debug("Could not register file with id "+file.getId()+" because it is still registered with another thread.");
						return false;
					}
				}
			}
			
			logger.info("Registering file with id "+file.getId()+" with HeartbeatManager...");
			// get a new connection to the database. Important because transactions are used and other java threads should not end up using the same connection.
			Connection dbConnection = DbHelper.getMainDb().getConnection();
			try {
				dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
				PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id=? FOR UPDATE");
				s.setInt(1, file.getId());
				ResultSet r = s.executeQuery();
				if (!r.next()) {
					logger.debug("Error trying to register file with id "+file.getId()+". It could not be found. Could have just been deleted.");
					dbConnection.prepareStatement("COMMIT").executeUpdate();
					s.close();
					dbConnection.close();
					return false;
				}
				
				// check that that the heartbeat hasn't been updates somewhere else
				Timestamp lastHeartbeat = r.getTimestamp("heartbeat");
				s.close();
				if (lastHeartbeat != null && lastHeartbeat.getTime() >= getProcessingFilesTimestamp().getTime()) {
					logger.debug("Could not register file with id "+file.getId()+" because it appears that it has been updated somewhere else.");
					dbConnection.prepareStatement("COMMIT").executeUpdate();
					dbConnection.close();
					return false;
				}
				
				// set the timestamp
				s = dbConnection.prepareStatement("UPDATE files SET heartbeat=? WHERE id=?");
				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				s.setTimestamp(1, currentTimestamp);
				s.setInt(2,  file.getId());
				int result = s.executeUpdate();
				s.close();
				if (result != 1) {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					dbConnection.close();
					return false;
				}
				dbConnection.prepareStatement("COMMIT").executeUpdate();
				dbConnection.close();
			} catch (SQLException e) {
				try {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					dbConnection.close();
				} catch (SQLException e1) {
					logger.debug("Transaction failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
				}
				throw(new RuntimeException("Error trying to register a file with HeartbeatManager."));
			}		

			files.add(new FileAndCounter(file));
		}
		logger.info("Registered file with id "+file.getId()+" with HeartbeatManager.");
		return true;
	}
	
	// returns true of the file is currently registered with the heartbeat manager
	public boolean isFileRegistered(File file) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter: files) {
				if (fileAndCounter.getFile() == file) {
					return true;
				}
			}
			return false;
		}
	}
	
	// un register a file that is no longer processing
	// if this file has been registered several times this will do nothing until called the last time
	public void unRegisterFile(File file) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					if (fileAndCounter.unRegister()) {
						// the counter has reached 0 so the file should be completely unregistered now
						files.remove(fileAndCounter);
					}
					return;
				}
			}
			throw(new RuntimeException("The file is not registered."));
		}
	}
	
	// returns the Timestamp that should be used to determine if a file is being processed
	// files with timestamps less than this value are no longer being processed
	public Timestamp getProcessingFilesTimestamp() {
		return new Timestamp(System.currentTimeMillis() - updateInterval - 5000);
	}
	
	private class Task extends TimerTask {
		
		private Connection dbConnection;
		
		public Task() {
			dbConnection = DbHelper.getMainDb().getConnection();
		}
		
		@Override
		public synchronized void run() {
			logger.debug("Updating heartbeat timestamps...");
			if (files.isEmpty()) {
				logger.debug("No files processing that need timestamps updating.");
				return;
			}
			
			synchronized(lock1) {
				String fileIdsWhere = "";
				fileIdsWhere = "(";
				for (int i=0; i<files.size(); i++) {
					if (i > 0) {
						fileIdsWhere += ",";
					}
					fileIdsWhere += "?";
				}
				fileIdsWhere += ")";
				
				try {
					PreparedStatement s = dbConnection.prepareStatement("UPDATE files SET heartbeat=? WHERE id IN "+fileIdsWhere);
					Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
					int paramNo = 1;
					s.setTimestamp(paramNo++, currentTimestamp);
					for (FileAndCounter fileAndCounter : files) {
						File file = fileAndCounter.getFile();
						s.setInt(paramNo++, file.getId());
					}
					if (s.executeUpdate() != files.size()) {
						logger.warn("Error occurred when updating heartbeat timestamps.");
					}
					s.close();
				
				} catch (SQLException e) {
					throw(new RuntimeException("Error trying to update files from HeartbeatManager."));
				}
			}
			logger.debug("Finished updating heartbeat timestamps.");
		}
		
	}
	
	private class FileAndCounter {
		private final File file;
		private int counter = 1;
		// store the thread id that first got the lock
		private final long threadId;

		public FileAndCounter(File file) {
			this.file = file;
			this.threadId = Thread.currentThread().getId();
		}
		
		// returns true if the thread calling this method matches the thread that created this object
		public boolean wasCreatedByCurrentThread() {
			return Thread.currentThread().getId() == threadId;
		}
		
		public void register() {
			if (!wasCreatedByCurrentThread()) {
				throw(new RuntimeException("The lock for this file was obtained by a different thread. Only the thread that created this may reregister the lock."));
			}
			counter++;
		}
		
		public boolean unRegister() {
			if (!wasCreatedByCurrentThread()) {
				throw(new RuntimeException("The lock for this file was obtained by a different thread. Only the thread that created this may unregister the lock."));
			}
			counter--;
			if (counter < 0) {
				throw(new RuntimeException("The counter should never go below 0. Unregister has been called too many times."));
			}
			return counter == 0;
		}
		
		public File getFile() {
			return file;
		}
	}
}
