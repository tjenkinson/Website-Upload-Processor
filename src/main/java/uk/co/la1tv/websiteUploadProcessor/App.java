package uk.co.la1tv.websiteUploadProcessor;

import org.apache.commons.configuration.ConfigurationException;

import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;

/**
 * Entry point for app.
 *
 */
public class App 
{
	/*
	 * First argument should be path to .properties config file (with extension)
	 */
    public static void main(String[] args) {
    	
    	if (args.length < 1) {
    		throw(new RuntimeException("Missing 1st argument for config file location."));
    	}
    	try {
			Config.init(FileHelper.format(args[0]));
		} catch (ConfigurationException e) {
			throw(new RuntimeException("Unable to load config."));
		}
    	
        System.out.println("Hello World 3!");
        System.out.println(Config.getInstance().getString("testConfigItem"));

    }
}
