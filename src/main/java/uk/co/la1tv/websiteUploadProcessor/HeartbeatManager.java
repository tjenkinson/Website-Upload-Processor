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

public class HeartbeatManager {
	
	private static Logger logger = Logger.getLogger(HeartbeatManager.class);
	
	private static HeartbeatManager instance = null;
	
	private final Timer timer;
	private final Config config;
	private final HashSet<FileAndCounter> files;
	private final long updateInterval;
	private final Object lock1 = new Object();
	private final int leewayTime = 10; // seconds
	
	private HeartbeatManager() {
		logger.info("Loading HeartbeatManager...");
		config = Config.getInstance();
		timer = new Timer(false);
		files = new HashSet<FileAndCounter>();
		int proposedUpdateInterval = config.getInt("general.heartbeatInterval");
		final int minumumUpdateInterval = 10+leewayTime;
		if (proposedUpdateInterval < minumumUpdateInterval) {
			logger.warn("heartbeatInterval should be at least "+minumumUpdateInterval+" seconds. "+minumumUpdateInterval+" seconds will be used.");
			proposedUpdateInterval = minumumUpdateInterval;
		}
		updateInterval = proposedUpdateInterval*1000;
		// make the actual update task at the heartbeat interval minus some leeway
		// this means if there is some delay for whatever reason before the schedule when the update should happen it is fine if it is within this leeway.
		// this is checked
		long updateTaskInterval = updateInterval - (leewayTime * 1000);
		timer.scheduleAtFixedRate(new Task(), 0, updateTaskInterval);
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
		return registerFile(file, false, Thread.currentThread());
	}
	
	public boolean registerFile(File file, boolean bypassCheck) {
		return registerFile(file, bypassCheck, Thread.currentThread());
	}
	
	// if bypassCheck is true this file will always be registered. There will be no check to see if the file is registered somewhere else.
	// if a file record is created on this server then it should be created with the heartbeat set to the current timestamp so no other servers can pick it up right from creation
	// in this case bypassCheck must be true to actually get it registered so the timestamp is kept updated.
	public boolean registerFile(File file, boolean bypassCheck, Object lockObj) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					// file already registered
					if (fileAndCounter.hasMatchingLock(lockObj)) {
						// increment the counter instead.
						fileAndCounter.register();
						return true;
					}
					else {
						// a the lockObj that was used when this file was created is different to the one provided now.
						logger.debug("Could not register file with id "+file.getId()+" because it is registered somewhere else in the application.");
						return false;
					}
				}
			}
			
			logger.info("Registering file with id "+file.getId()+" with HeartbeatManager...");
			// get a new connection to the database. Important because transactions are used and other java threads should not end up using the same connection.
			Connection dbConnection = DbHelper.getMainDb().getConnection();
			if (dbConnection == null) {
				// could not connect for some reason#
				logger.warn("Error trying to register file with id "+file.getId()+". Could not get connection to database.");
				return false;
			}
			try {
				dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
				// getting an exclusive lock is important as it makes sure we are reading the latest timestamp
				PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id=? FOR UPDATE");
				s.setInt(1, file.getId());
				long timeRequestMade = System.currentTimeMillis();
				ResultSet r = s.executeQuery();
				if (!r.next()) {
					logger.debug("Error trying to register file with id "+file.getId()+". It could not be found. Could have just been deleted.");
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
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
				Long lastHeartbeatTime = lastHeartbeat != null ? lastHeartbeat.getTime() + timeTakenToGetResponse : null;
				s.close();
				// check that that the heartbeat hasn't been updates somewhere else
				if (!bypassCheck && lastHeartbeatTime != null && lastHeartbeatTime >= getProcessingFilesTimestamp().getTime()) {
					logger.debug("Could not register file with id "+file.getId()+" because it appears that it has been updated somewhere else.");
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
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
				e.printStackTrace();
				try {
					dbConnection.prepareStatement("ROLLBACK").executeUpdate();
				} catch (SQLException e1) {
					logger.debug("Transaction failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
				}
				try {
					dbConnection.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				throw(new RuntimeException("Error trying to register a file with HeartbeatManager."));
			}		

			files.add(new FileAndCounter(file, lockObj));
		}
		logger.info("Registered file with id "+file.getId()+" with HeartbeatManager.");
		return true;
	}
	
	public void switchLockObj(File file, Object currentLockObj) {
		switchLockObj(file, currentLockObj, Thread.currentThread());
	}
	
	// switch the lockobj that is currently associated with a registered file
	public void switchLockObj(File file, Object currentLockObj, Object newLockObj) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					if (fileAndCounter.hasMatchingLock(currentLockObj)) {
						fileAndCounter.updateLockObj(newLockObj);
						return;
					}
					else {
						throw(new RuntimeException("Could not switch lockobj on file because the current lockobj does not match."));
					}
				}
			}
			throw(new RuntimeException("Could not switch lockobj on file because the file is not registered."));
		}
	}
	
	// returns true of the file is currently registered with the heartbeat manager
	public boolean isFileRegistered(File file) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					return true;
				}
			}
			return false;
		}
	}
	
	public void unRegisterFile(File file) {
		unRegisterFile(file, Thread.currentThread());
	}
	
	// un register a file that is no longer processing
	// if this file has been registered several times this will do nothing until called the last time
	public void unRegisterFile(File file, Object lockObj) {
		synchronized(lock1) {
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					if (fileAndCounter.hasMatchingLock(lockObj)) {
						if (fileAndCounter.unRegister()) {
							// the counter has reached 0 so the file should be completely unregistered now
							// TODO: double check still have lock, then set the timestamp to NULL so that other servers can pick it up immediately. At the moment other servers have to wait for it to timeout
							files.remove(fileAndCounter);
							logger.info("Unregistered file with id "+file.getId()+" from heartbeat manager.");
						}
					}
					else {
						throw(new RuntimeException("Cannot unregister file because the lockObj doesn't match the one it was created with."));
					}
					return;
				}
			}
			// the file is not registered. may have been forcibly unregistered though so don't throw an exception, just ignore it.
		}
	}
	
	// if the heartbeat manager can no longer guarantee it has the file registered it will unregister it.
	// the main program code should use isFileRegistered to check if the file is still registered before performing tasks which require exclusivity (probably whilst in a database transaction with an exclusive lock)
	private void forciblyUnregisterFile(File file) {
		synchronized(lock1) {
			FileAndCounter toRemove = null;
			for(FileAndCounter fileAndCounter : files) {
				if (fileAndCounter.getFile() == file) {
					toRemove = fileAndCounter;
					break;
				}
			}
			
			if (toRemove != null) {
				files.remove(toRemove);
				logger.warn("Forcibly unregistered file with id "+file.getId()+" as can no longer guarantee exclusive access for some reason.");
			}
		}
	}
	
	// returns the Timestamp that should be used to determine if a file is being processed
	// files with timestamps less than this value are no longer being processed
	public Timestamp getProcessingFilesTimestamp() {
		return new Timestamp(System.currentTimeMillis() - updateInterval - 30000);
	}
	
	private class Task extends TimerTask {
		
		@Override
		public synchronized void run() {
			
			try {
				logger.debug("Updating heartbeat timestamps...");
				if (files.isEmpty()) {
					logger.debug("No files processing that need timestamps updating.");
					return;
				}
				
				synchronized(lock1) {
					
					Connection dbConnection = DbHelper.getMainDb().getConnection();
					for (FileAndCounter fileAndCounter : files) {
						File file = fileAndCounter.getFile();
					
						if (dbConnection == null) {
							// error connecting to database
							// therefore can't update timestamp so can't guarantee this file is only registered with this server so forcivly unregister it
							forciblyUnregisterFile(file);
						}
						else {
							boolean unregisterFile = false;
							try {
								dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
								// first get an exclusive lock on the file record
								PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id=? FOR UPDATE");
								s.setInt(1, file.getId());
								s.executeQuery();
								ResultSet r = s.getResultSet();
								if (!r.next()) {
									// couldn't find the record for some reason
									// unregister the file
									logger.warn("Attempting to update timestamp for file with id "+file.getId()+" but could not find record.");
									unregisterFile = true;
								}
								s.close();
								
								// now that we have an exclusive lock check to see if it has been too long since the last update.
								// need to use the time locally that we last updated not the one in the record we just received because if has been too long, that time might have been updated somewhere else
								// if this is the case unregister the file
								if (fileAndCounter.timeHeartbeatLastUpdated != null && fileAndCounter.timeHeartbeatLastUpdated + updateInterval < System.currentTimeMillis()) {
									// the update interval has passed since the last update so it can no longer be guarenteed that another server hasn't picked up the file.
									unregisterFile = true;
								}
								
								if (unregisterFile) {
									forciblyUnregisterFile(file);
									dbConnection.prepareStatement("ROLLBACK").executeUpdate();
								}
								else {
									// now that we have an exclusive lock we can be confident that this query will execute pretty instantly and therefore the time will be accurate.
									// if we didn't get the lock above then this update command would need to get an exclusive lock, which could take some time, meaning then when it gets the lock the time that would be written would be old
									// whenever a server tries to register a file they first request an exclusive lock on the record.
									// provided that all requests to the mysql server with exclusive locks are handled in the order that the locks were requested, there should be no issues
									Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
									s = dbConnection.prepareStatement("UPDATE files SET heartbeat=? WHERE id=?");
									s.setTimestamp(1, currentTimestamp);
									s.setInt(2, file.getId());
									if (s.executeUpdate() != 1) {
										logger.error("Error occurred when updating heartbeat timestamp for file with id "+file.getId()+".");
										dbConnection.prepareStatement("ROLLBACK").executeUpdate();
										// can no longer guarantee this file is registered with this server so unregister it
										forciblyUnregisterFile(file);
									}
									else {
										dbConnection.prepareStatement("COMMIT").executeUpdate();
										fileAndCounter.timeHeartbeatLastUpdated = System.currentTimeMillis();
										logger.debug("Updated heartbeat timestamp for file with id "+file.getId()+".");
									}
									s.close();
								}
							
							} catch (SQLException e) {
								logger.error("SQLException occurred when updating heartbeat timestamp for file with id "+file.getId()+".");
								e.printStackTrace();
								try {
									dbConnection.prepareStatement("ROLLBACK").executeUpdate();
								} catch (SQLException e1) {
									logger.debug("Transaction for updating heartbeat timestamps failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
								}
								
								// can no longer guarantee this file is registered with this server so unregister it
								forciblyUnregisterFile(file);
							}
						}
					}
					if (dbConnection != null) {
						try {
							dbConnection.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
				logger.debug("Finished updating heartbeat timestamps.");
			}
			catch(Exception e) {
				// if there is an exception that has not been handled occuring in the heartbeat task then regard this as fatal and terminate the app.
				e.printStackTrace();
				logger.fatal("An exception occurred in the heartbeat timer task and therefore the application is being terminated.");
				System.exit(1);
			}
		}
	}
	
	private class FileAndCounter {
		private final File file;
		private int counter = 1;
		// an object reference which will be provided when the file is registered and only the same reference will work for unregistering
		private Object lockObj;
		public Long timeHeartbeatLastUpdated = null;

		public FileAndCounter(File file, Object lockObj) {
			this.file = file;
			this.lockObj = lockObj;
		}
		
		// returns true if the thread calling this method matches the thread that created this object
		public boolean hasMatchingLock(Object lockObj) {
			return this.lockObj == lockObj;
		}
		
		public void updateLockObj(Object newLockObj) {
			lockObj = newLockObj;
		}
		
		public void register() {
			counter++;
		}
		
		public boolean unRegister() {
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
