package uk.co.la1tv.websiteUploadProcessor;

import org.apache.commons.configuration.ConfigurationException;

import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

/**
 * Entry point for app.
 * 
 */
public class App {
	/**
	 * First argument should be path to the config file.
	 * Config file format: http://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html#Properties_files
	 */
	public static void main(String[] args) {

		if (args.length < 1) {
			throw (new RuntimeException(
					"Missing 1st argument for config file location."));
		}
		
		// initialise config
		try {
			Config.init(FileHelper.format(args[0]));
		} catch (ConfigurationException e) {
			throw (new RuntimeException("Unable to load config."));
		}
		
		Config config = Config.getInstance();
		
		// connect to database and pass Db object to DbHelper so it can be retrieved from anywhere
		DbHelper.setMainDb(new Db(config.getString("db.host"), config.getString("db.database"), config.getString("db.username"), config.getString("db.password")));

		
		System.out.println("Hello World 4!");

	}
}
