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
				// getting an exclusive lock is important as it makes sure we are reading the latest timestamp
				PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id=? FOR UPDATE");
				s.setInt(1, file.getId());
				long timeRequestMade = System.currentTimeMillis();
				ResultSet r = s.executeQuery();
				if (!r.next()) {
					logger.debug("Error trying to register file with id "+file.getId()+". It could not be found. Could have just been deleted.");
					dbConnection.prepareStatement("COMMIT").executeUpdate();
					s.close();
					dbConnection.close();
					return false;
				}
				
				long timeTakenToGetResponse = System.currentTimeMillis() - timeRequestMade;
				
				Timestamp lastHeartbeat = r.getTimestamp("heartbeat");
				
				// have to presume whilst this was waiting for the lock to read the timestamp, the server with is currently processing the file was unable to update the time, because it is also waiting for a lock.
				// e.g.
				// server 1 already has registered a file
				// server 1 get exclusive lock on file record and updates the timestamp on the file record at time 10
				// server 2 makes query to get the current timestamp from the file record at time 11
				// for whatever reason the file record is currently locked so server 2 is still waiting to get the time
				// server 1 makes query to get lock on the file record, so that it can then update it, at time 40. It can't get a lock at the moment as it is already locked somewhere else
				// the file record becomes unlocked, so then server 2 gets the lock as it was next in line, and gets the result, at time 45. The result it would get is 10
				// server 2 now releases the lock and server 1 gets it and writes in the current time at time 46
				
				// therefore adding the time taken to get the response to the time that is received works
				long lastHeartbeatTime = lastHeartbeat.getTime() + timeTakenToGetResponse;
				s.close();
				// check that that the heartbeat hasn't been updates somewhere else
				if (lastHeartbeat != null && lastHeartbeatTime >= getProcessingFilesTimestamp().getTime()) {
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
				
				// TODO: the exception could be due to a lock timeout. need to kill any threads that think they have the lock
				
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
		return new Timestamp(System.currentTimeMillis() - updateInterval - 30000);
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
					dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
					// first get an exclusive lock on all the records that the timestamps need updating on
					PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id IN "+fileIdsWhere+" FOR UPDATE");
					int paramNo = 1;
					for (FileAndCounter fileAndCounter : files) {
						File file = fileAndCounter.getFile();
						s.setInt(paramNo++, file.getId());
					}
					s.executeQuery();
					
					// now that we have an exclusive lock we can be confident that this query will execute pretty instantly and therefore the time will be accurate.
					// if we didn't get the lock above then this update command would need to get an exclusive lock, which could take some time, meaning then when it gets the lock the time that would be written would be old
					// whenever a server tries to register a file they first request an exclusive lock on the record.
					// provided that all requests to the mysql server with exclusive locks are handled in the order that the locks were requested, there should be no issues
					Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
					s = dbConnection.prepareStatement("UPDATE files SET heartbeat=? WHERE id IN "+fileIdsWhere);
					paramNo = 1;
					s.setTimestamp(paramNo++, currentTimestamp);
					for (FileAndCounter fileAndCounter : files) {
						File file = fileAndCounter.getFile();
						s.setInt(paramNo++, file.getId());
					}
					if (s.executeUpdate() != files.size()) {
						logger.warn("Error occurred when updating heartbeat timestamps. Some may not have been updated.");
					}
					dbConnection.prepareStatement("COMMIT").executeUpdate();
					
					s.close();
				
				} catch (SQLException e) {
					try {
						dbConnection.prepareStatement("ROLLBACK").executeUpdate();
					} catch (SQLException e1) {
						logger.debug("Transaction for updating heartbeat timestamps failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
					}
					throw(new RuntimeException("Error trying to update files with HeartbeatManager."));
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
