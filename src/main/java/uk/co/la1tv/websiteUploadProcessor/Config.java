package uk.co.la1tv.websiteUploadProcessor;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Config extends PropertiesConfiguration {
	
	private static Config instance = null;
	
	private Config(String location) throws ConfigurationException {
		super(location);
	}
	
	public static void init(String location) throws ConfigurationException {
		if (instance != null) {
			throw(new RuntimeException("Config already initialised!"));
		}
		instance = new Config(location);
	}
	
	public static Config getInstance() {
		if (instance == null) {
			throw(new RuntimeException("Config not initialised yet."));
		}
		return instance;
	}
}
