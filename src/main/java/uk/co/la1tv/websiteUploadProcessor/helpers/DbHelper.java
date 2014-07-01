package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.sql.ResultSet;
import java.sql.SQLException;

import uk.co.la1tv.websiteUploadProcessor.Db;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;

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
	
	/**
	 * Builds a file object from a result set's row.
	 * @param r: The result set on the required row.
	 * @return File: A File object representing the db record.
	 * @throws SQLException
	 */
	public static File buildFileFromResult(ResultSet r) throws SQLException {
		return new File(r.getInt("id"), r.getString("filename"), r.getInt("size"), FileType.getFromId(r.getInt("file_type_id")));
	}
	
}
