package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class FfmpegProgressMonitor {
	
	private static Logger logger = Logger.getLogger(FfmpegProgressMonitor.class);

	private final Timer timer;
	private final File progressFile;
	private final double totalNoFrames;
	private double currentFrame = 0;
	private boolean fileCreated = false;
	private Runnable callback = null;
	
	/**
	 * Polls the progress file generated from ffmpeg and provides an easy way of getting progress info.
	 * Progress file written from ffmpeg with the -progress option.
	 * 
	 * Make sure to call destroy to make sure timer is cancelled.
	 * 
	 * @param progressFile: the file that ffmpeg is writing the progress updates into.
	 * @param totalNoFrames: the total number of frames in the output file. Used to calculate the percentage.
	 */
	public FfmpegProgressMonitor(File progressFile, double totalNoFrames) {
		this.totalNoFrames = totalNoFrames;
		this.progressFile = progressFile;
		
		timer = new Timer();
		timer.schedule(new ReadFileTask(), 0, 800);
	}
	
	// set callback to be run when progress is updated
	public void setCallback(Runnable callback) {
		this.callback = callback;
	}
	
	// stops timer that is checking file (if it is still running)
	public void destroy() {
		timer.cancel();
	}
	
	// returns percentage (0-100)
	public int getPercentage() {
		return (int) Math.floor(((float) (currentFrame*100)) / totalNoFrames);
	}
	
	private class ReadFileTask extends TimerTask {

		@Override
		public synchronized void run() {
			
			if (!fileCreated && progressFile.exists()) {
				fileCreated = true;
			}
			
			// check if file still exists and if it doesn't cancel the timer.
			if (fileCreated && !progressFile.exists()) {
				logger.info("Cancelling timer because progress file no longer exists.");
				timer.cancel();
				return;
			}
			
			@SuppressWarnings("rawtypes")
			List lines;
			try {
				lines = FileUtils.readLines(progressFile);
			} catch (IOException e1) {
				logger.trace("Error trying to generate config from ffmpeg progress file. Maybe file is being written at the same time as this check?");
				return;
			}
			
			if (lines.size() < 2 || !getLineParts(lines, lines.size()-1)[0].equals("progress")) {
				// presume file currently being written
				return;
			}
			
			if (getLineParts(lines, lines.size()-1)[1].equals("end")) {
				// ffmpeg process finished so don't keep polling after this one
				// still get the latest result though
				logger.info("Cancelling timer because ffmpeg finished.");
				timer.cancel();
			}
			
			// loop through lines starting from end to find the line after the last progress= which is where the last update started
			// don't look at last line. we already know it's process
			double frame = -1;
			boolean foundFrame = false;
			for (int i=lines.size()-2; i>=0; i--) {
				String[] parts = getLineParts(lines, i);
				if (parts[0].equals("progress")) {
					// reached the start of the last update
					break;
				}
				if (parts[0].equals("frame")) {
					frame = Double.parseDouble(parts[1]);
					foundFrame = true;
				}
			}
			
			if (!foundFrame) {
				throw (new RuntimeException("Ffmpeg did not write frame attribute in progress file."));
			}
			
			if (currentFrame == frame) {
				return;
			}
			currentFrame = frame;
			if (callback != null) {
				// notify callback that progress changed
				callback.run();
			}
		}
		
		private String[] getLineParts(@SuppressWarnings("rawtypes") List lines, int row) {
			return ((String) lines.get(row)).split("=");
		}
		
	}
}
