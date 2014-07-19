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
	
	private final Timer timer;
	private final Config config;
	private final HashSet<File> files;
	private final long updateInterval;
	private final Db db;
	private final Object lock1 = new Object();
	
	public HeartbeatManager() {
		logger.info("Loading HeartbeatManager...");
		config = Config.getInstance();
		timer = new Timer(false);
		files = new HashSet<File>();
		updateInterval = config.getInt("general.heartbeatInterval")*1000;
		db = DbHelper.getMainDb();
		timer.schedule(new Task(), 0, updateInterval);
		logger.info("Loaded HeartbeatManager.");
	}
	
	// register a file that is processing
	// returns true if the file was successfully registered.
	// could be false if the same file is registered at the same time from different servers. Only one will win.
	public boolean registerFile(File file) {
		logger.info("Registering file with id "+file.getId()+" with HeartbeatManager...");
		Connection dbConnection = db.getConnection();
		try {
			dbConnection.prepareStatement("START TRANSACTION").executeUpdate();
			PreparedStatement s = dbConnection.prepareStatement("SELECT heartbeat FROM files WHERE id=? FOR UPDATE");
			s.setInt(1, file.getId());
			ResultSet r = s.executeQuery();
			if (!r.next()) {
				logger.debug("Error trying to register file with id "+file.getId()+". It could not be found. Could have just been deleted.");
				dbConnection.prepareStatement("COMMIT").executeUpdate();
				return false;
			}
			
			// check that that the heartbeat hasn't been updates somewhere else
			Timestamp lastHeartbeat = r.getTimestamp("heartbeat");
			if (lastHeartbeat != null && lastHeartbeat.getTime() >= getProcessingFilesTimestamp().getTime()) {
				logger.debug("Could not register file with id "+file.getId()+" because it appears that it has been updated somewhere else.");
				dbConnection.prepareStatement("COMMIT").executeUpdate();
				return false;
			}
			
			// set the timestamp
			s = dbConnection.prepareStatement("UPDATE files SET heartbeat=? WHERE id=?");
			Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
			s.setTimestamp(1, currentTimestamp);
			s.setInt(2,  file.getId());
			if (s.executeUpdate() != 1) {
				dbConnection.prepareStatement("ROLLBACK").executeUpdate();
				return false;
			}
			dbConnection.prepareStatement("COMMIT").executeUpdate();
		} catch (SQLException e) {
			try {
				dbConnection.prepareStatement("ROLLBACK").executeUpdate();
			} catch (SQLException e1) {
				logger.debug("Transaction failed to be rolled back. This is possible if the reason is that the transaction failed to start in the first place.");
			}
			throw(new RuntimeException("Error trying to register a file with HeartbeatManager."));
		}		
		
		synchronized(lock1) {
			files.add(file);
		}
		logger.info("Registered file with id "+file.getId()+" with HeartbeatManager.");
		return true;
	}
	
	// un register a file that is no longer processing
	public void unRegisterFile(File file) {
		synchronized(lock1) {
			files.remove(file);
		}
	}
	
	// returns the Timestamp that should be used to determine if a file is being processed
	// files with timestamps less than this value are no longer being processed
	public Timestamp getProcessingFilesTimestamp() {
		return new Timestamp(System.currentTimeMillis() - updateInterval - 5000);
	}
	
	private class Task extends TimerTask {

		@Override
		public synchronized void run() {
			logger.debug("Updating heartbeat timestamps...");
			if (files.isEmpty()) {
				logger.debug("No files processing that need timestamps updating.");
				return;
			}
			
			Connection dbConnection = db.getConnection();
			
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
					for (File file : files) {
						s.setInt(paramNo++, file.getId());
					}
					if (s.executeUpdate() != files.size()) {
						logger.warn("Error occurred when updating heartbeat timestamps.");
					}
				
				} catch (SQLException e) {
					throw(new RuntimeException("Error trying to update files from HeartbeatManager."));
				}
			}
			logger.debug("Finished updating heartbeat timestamps.");
		}
		
	}
}
