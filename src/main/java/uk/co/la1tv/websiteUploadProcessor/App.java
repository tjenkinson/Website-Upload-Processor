package uk.co.la1tv.websiteUploadProcessor;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

/**
 * Entry point for app.
 * 
 */
public class App {
	
	private static Logger logger = Logger.getLogger(App.class);
	
	/**
	 * First argument should be path to the log4j config file.
	 * Second argument should be path to the main config file.
	 * Config file format: http://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html#Properties_files
	 */
	public static void main(String[] args) {
		
		if (args.length < 2) {
			throw (new RuntimeException("Missing arguments. Argument 1 is log4J configuration path and argument 2 is main application config path."));
		}
	
		// setup Log4J
		PropertyConfigurator.configure(FileHelper.format(args[0]));
		logger.info("Application started.");

		// initialise config
		try {
			Config.init(FileHelper.format(args[1]));
		} catch (ConfigurationException e) {
			throw (new RuntimeException("Unable to load config."));
		}
		
		Config config = Config.getInstance();
		
		// connect to database and pass Db object to DbHelper so it can be retrieved from anywhere
		DbHelper.setMainDb(new Db(config.getString("db.host"), config.getString("db.database"), config.getString("db.username"), config.getString("db.password")));

		new JobPoller();
	}
}
