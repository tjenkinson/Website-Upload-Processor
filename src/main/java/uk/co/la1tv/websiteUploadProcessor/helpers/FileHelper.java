package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;

public class FileHelper {
	
	private static Logger logger = Logger.getLogger(FileHelper.class);
	
	private FileHelper() {}
	
	/**
	 * Formats the path passed in so that it is correct for the filesystem it's running on. 
	 * @param path
	 * @return Formatted path
	 */
	public static String format(String path) {
		char sep = System.getProperty("file.separator").charAt(0);
		return path.replace(sep == '\\' ? '/' : '\\', sep);
	}
	
	/**
	 * Empties the working directory. Also creates it if it doesn't exist.
	 */
	public static void cleanWorkingDir() {
		String workingDirPath = FileHelper.format(Config.getInstance().getString("files.workingFilesLocation"));
		if (Files.exists(Paths.get(workingDirPath), LinkOption.NOFOLLOW_LINKS)) {
			logger.info("Cleaning working directory...");
			try {
				FileUtils.cleanDirectory(new File(workingDirPath));
			} catch (IOException e) {
				throw(new RuntimeException("Error occurred when trying to clear working directory."));
			}
			logger.info("Cleaned working directory.");
		}
		else {
			logger.info("Working directory doesn't exist. Creating it...");
			try {
				FileUtils.forceMkdir(new File(workingDirPath));
			} catch (IOException e) {
				throw(new RuntimeException("Error occurred when trying to create working directory."));
			}
			logger.info("Created working directory.");
		}
	}
	
	public static String getWorkingDir() {
		return FileHelper.format(Config.getInstance().getString("files.workingFilesLocation"));
	}
	
	public static String getFileWorkingDir(int fileId) {
		return FileHelper.format(FileHelper.getWorkingDir()+"/"+fileId);
	}
	
	public static String getSourceFilePath(int fileId) {
		return FileHelper.format(Config.getInstance().getString("files.webappFilesLocation")+"/"+fileId);
	}

	public static String getSourcePendingFilePath(int fileId) {
		return FileHelper.format(Config.getInstance().getString("files.webappPendingFilesLocation")+"/"+fileId);
	}
	
	public static boolean moveToWebApp(File source, int id) {
		File destinationLocation = new File(FileHelper.format(Config.getInstance().getString("files.webappFilesLocation")+"/"+id));
		destinationLocation.delete(); // delete file at destination (if there is one)
		return source.renameTo(destinationLocation);
	}
	
	public static boolean isOverQuota() {
		return isOverQuota(BigInteger.ZERO);
	}
	
	public static boolean isOverQuota(BigInteger additional) {
		Config config = Config.getInstance();		
		return FileUtils.sizeOfAsBigInteger(new File(FileHelper.format(config.getString("files.webappFilesLocation")))).compareTo(config.getBigInteger("general.webAppSpaceQuota").multiply(new BigInteger("1000000")).add(additional)) > 0 ;
	}
}
