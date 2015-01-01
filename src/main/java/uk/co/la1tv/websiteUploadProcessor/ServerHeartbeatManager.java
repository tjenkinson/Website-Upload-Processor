package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;

/**
 * Responsible for making sure there's an entry in the
 * servers table for this server and that the heartbeat
 * field is kept up to date.
 *
 */
public class ServerHeartbeatManager {

	private static Logger logger = Logger.getLogger(ServerHeartbeatManager.class);
	
	private final Timer timer;
	private final Config config;
	private final int serverId;
	private final long updateInterval;
	
	public ServerHeartbeatManager() {
		logger.info("Loading ServerHeartbeatManager...");
		config = Config.getInstance();
		serverId = config.getInt("server.id");
		
		// create server record if it doesn't exist
		Connection dbConnection = DbHelper.getMainDb().getConnection();
		if (dbConnection == null) {
			throw(new RuntimeException("Error connecting to database when starting server heartbeat manager."));
		}
		
		PreparedStatement s = null;
		try {
			s = dbConnection.prepareStatement("SELECT count(*) AS count FROM processing_servers WHERE id=?");
			s.setInt(1, serverId);
			ResultSet r = s.executeQuery();
			r.next();
			int count = r.getInt("count");
			s.close();
			if (count == 0) {
				// server record doesn't exist
				logger.info("No entry for this server in the database. Creating one...");
				try {
					s = dbConnection.prepareStatement("INSERT INTO processing_servers SET id=?, heartbeat=?");
					Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
					s.setInt(1, serverId);
					s.setTimestamp(2, currentTimestamp);
					if (s.executeUpdate() != 1) {
						s.close();
						throw(new RuntimeException("Error when trying to create entry for server in database."));
					}
					s.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw(new RuntimeException("Error when trying to create entry for server in database."));
				}
				logger.info("Created entry for this server in the database.");
			}
			dbConnection.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw(new RuntimeException("Error when trying to start server heartbeat manager."));
		}
		
		timer = new Timer(false);
		updateInterval = config.getInt("general.heartbeatInterval")*1000;
		timer.scheduleAtFixedRate(new Task(), 0, updateInterval);
		logger.info("Loaded ServerHeartbeatManager.");
	}
	
	
	private class Task extends TimerTask {
			
			@Override
			public synchronized void run() {
				try {
					logger.debug("Updating server heartbeat timestamp...");	
					Connection dbConnection = DbHelper.getMainDb().getConnection();
					if (dbConnection == null) {
						logger.error("Error connecting to database when trying to update heartbeat timestamp. Timestamp won't be updated this time.");
					}
					else {
						Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
						PreparedStatement s;
						try {
							s = dbConnection.prepareStatement("UPDATE processing_servers SET heartbeat=? WHERE id=?");
							s.setTimestamp(1, currentTimestamp);
							s.setInt(2, serverId);
							if (s.executeUpdate() != 1) {
								logger.warn("No records were effected when trying to update the timestamp for the server.");
							}
							s.close();
						} catch (SQLException e) {
							logger.error("SQLException when trying to update server heartbeat timestamp.");
							e.printStackTrace();
						}
					}
					
					if (dbConnection != null) {
						try {
							dbConnection.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					logger.debug("Updated server heartbeat timestamp.");
				}
				catch (Exception e) {
					logger.error("Exception occurred whilst trying to update server heartbeat timestamp.");
					e.printStackTrace();
				}
			}
		
	}

}