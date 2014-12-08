package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.util.HashSet;
import java.util.Set;

import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.HeartbeatManager;

public class FileTypeProcessReturnInfo {
	public String msg = null;
	
	// stores new files that have been created and will need marking as in_use, and will also need unregistering from the heartbeat monitor when the job is done 
	private Set<File> newFiles = new HashSet<>();
	public boolean success = false;
	
	// add new files that are created here.
	// they will be registered with the heartbeat manager and also marked as in_use when the processing finishes (if is all successful)
	// returns false if the file could not be added because there was an issue registering with the heartbeat manager
	public boolean registerNewFile(File file) {
		// force this file to be registered with the heartbeat manager
		if (!HeartbeatManager.getInstance().registerFile(file, true)) {
			return false;
		}
		newFiles.add(file);
		return true;
	}
	
	public Set<File> getNewFiles() {
		return newFiles;
	}
}
