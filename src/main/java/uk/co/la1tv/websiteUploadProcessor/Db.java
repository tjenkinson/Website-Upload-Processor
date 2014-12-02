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
	}
	
	
	/**
	 * Get a new mysql connection.
	 * @return the Connection or null if a connection could not be made for some reason
	 */
	public Connection getConnection() {
		Connection connection = null;
		logger.info("Connecting to database.");
		try {
			connection = DriverManager.getConnection("jdbc:mysql://"+host+"/"+database, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (connection == null) {
			logger.warn("Could not connect to the database for some reason.");
		}
		else {
			logger.info("Connected to database.");
		}
		return connection;
	}
}
