package uk.co.la1tv.websiteUploadProcessor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class Db {
	
	private static Logger logger = Logger.getLogger(Db.class);
	
	private String host;
	private String database;
	private String username;
	private String password;
	
	private Connection connection = null;
	
	public Db(String host, String database, String username, String password) {
		this.host = host;
		this.database = database;
		this.username = username;
		this.password = password;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw(new RuntimeException("Could not load database driver."));
		}
		
		connect();
	}
	
	/**
	 * Connects to the database if not already connected.
	 */
	public void connect() {
		logger.info("Connecting to database.");
		if (connection != null) {
			logger.info("Already connected.");
			return;
		}
		
		Config config = Config.getInstance();
		for(int i=0; i<config.getInt("db.noConnectionRetries") && connection == null; i++) {
			if (i > 0) {
				logger.warn("Connection failed. Retrying in "+config.getInt("db.connectionRetryInterval")+" seconds. Attempt "+(i+1)+".");
				try {
					Thread.sleep(config.getInt("db.connectionRetryInterval")*1000);
				} catch (InterruptedException e) {
					throw(new RuntimeException("Thread was interruped whilst sleeping to retry database connection. This shouldn't happen!"));
				}
			}
			try {
				connection = DriverManager.getConnection("jdbc:mysql://"+host+"/"+database, username, password);
			} catch (SQLException e) {}
		}
		if (connection == null) {
			throw(new RuntimeException("Could not connect to database."));
		}
		logger.info("Connected to database.");
	}
	
	/**
	 * Disconnects from the database if connected.
	 */
	public void disconnect() {
		logger.info("Disconnecting from database.");
		if (connection == null) {
			logger.info("Already disconnected.");
			return;
		}
		try {
			connection.close();
		} catch (SQLException e) {
			throw(new RuntimeException("Error closing database connection."));
		}
		connection = null;
		logger.info("Disconnected from database.");
	}
	
	/**
	 * Get the Connection object associated with this Db object.
	 * @return the Connection
	 */
	public Connection getConnection() {
		return connection;
	}
}
