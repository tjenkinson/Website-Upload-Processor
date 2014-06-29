package uk.co.la1tv.websiteUploadProcessor.helpers;

import uk.co.la1tv.websiteUploadProcessor.Db;

public class DbHelper {
	
	private static Db db = null;
	
	private DbHelper() {}

	/**
	 * Set Db object that will be used through out the application.
	 * This means it can be retrieved from anywhere.
	 * @param db
	 */
	public static void setMainDb(Db db) {
		DbHelper.db = db;
	}
	
	/**
	 * Retrieve the main Db object that was initialised during application startup.
	 * @return The Db object
	 */
	public static Db getMainDb() {
		return db;
	}
	
}
