package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.io.File;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.commons.math3.fraction.FractionFormat;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import uk.co.la1tv.websiteUploadProcessor.Config;

public class FfmpegHelper {
	
	private static Logger logger = Logger.getLogger(FfmpegHelper.class);
	
	public static FfmpegFileInfo getFileInfo(File file, File workingDir) {
		
		Config config = Config.getInstance();
		int exitVal;
		
		// get source file information.
		GenericStreamMonitor streamMonitor = new GenericStreamMonitor();
		
		exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.probeLocation"), "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", "-select_streams", "v:0", "-count_frames", file.getAbsolutePath()}, workingDir, streamMonitor, null);
		if (exitVal != 0) {
			logger.warn("Error retrieving metadata for file '"+file.getAbsolutePath()+"' with ffprobe.");
			return null;
		}
		
		// the output from ffmpegprobe should be pure json
		JSONObject metadata;
		int w;
		int h;
		double frameRate;
		double duration;
		double noFrames;
		double size;
		
		try {
			metadata = new JSONObject(streamMonitor.getOutput());
			w = metadata.getJSONArray("streams").getJSONObject(0).getInt("width");
			h = metadata.getJSONArray("streams").getJSONObject(0).getInt("height");
			Fraction f = new FractionFormat().parse(metadata.getJSONArray("streams").getJSONObject(0).getString("r_frame_rate"));
			frameRate = f.doubleValue();
			duration = Double.parseDouble(metadata.getJSONObject("format").getString("duration"));
			noFrames = Double.parseDouble(metadata.getJSONArray("streams").getJSONObject(0).getString("nb_read_frames"));
			size = Double.parseDouble(metadata.getJSONObject("format").getString("size"));
		}
		catch(JSONException e) {
			logger.warn("Error parsing JSON from ffprobe for file '"+file.getAbsolutePath()+"'.");
			e.printStackTrace();
			return null;
		}
		
		return new FfmpegFileInfo(w, h, frameRate, duration, noFrames, size);
	}
	
	// create idealNumber number of thumbnails from the provided video. with a minimum of 1 per second
	// returns an array of the output files in order, or null if there was an error
	public static VideoThumbnail[] generateThumbnails(int idealNumber, File source, File workingDir, int w, int h) {
		FfmpegFileInfo info = getFileInfo(source, workingDir);
		if (info == null) {
			return null;
		}
		double duration = info.getDuration();
		int width = info.getW();
		int height = info.getH();
		int calculatedWidth = width;
		int calculatedHeight = height;
		// figure out what the width and height should be so it would fit in a box of wxh
		if (calculatedWidth > w) {
			calculatedWidth = w;
			calculatedHeight = (int) Math.floor((double) calculatedHeight / ((double) calculatedWidth/(double) w));
		}
		if (calculatedHeight > h) {
			calculatedHeight = h;
			calculatedWidth = (int) Math.floor((double) calculatedWidth / ((double) calculatedHeight/(double) h));
		}
		// interval in seconds that the thumbnails should be generated at
		int interval = Math.max(1, (int) Math.ceil(duration/idealNumber));
		
		// 1 extra because a thumbnail is also taken at 0 seconds
		int numThumbnails = (int) Math.floor(duration/interval) + 1;
		
		Config config = Config.getInstance();
		// the way the frame rate option works is that the first image will be 0,
		// the second will be interval/2
		// the third will be interval + (interval/2) 
		// etc
		int exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.location"), "-y", "-nostdin", "-i", source.getAbsolutePath(), "-vf", "fps=1/"+interval+",scale="+calculatedWidth+":"+calculatedHeight, workingDir.getAbsolutePath()+System.getProperty("file.separator")+"thumb_%d.jpg"}, workingDir, null, null);
		if (exitVal != 0) {
			logger.warn("Error generating video thumbnails for '"+source.getAbsolutePath()+"' with ffmpeg.");
			return null;
		}
		
		// generate the Files that correspond to the output files
		VideoThumbnail[] videoThumbnails = new VideoThumbnail[numThumbnails];
		for(int i=0; i<videoThumbnails.length; i++) {
			File file = new File(workingDir.getAbsolutePath()+System.getProperty("file.separator")+"thumb_"+(i+1)+".jpg");
			if (!file.exists()) {
				logger.warn("A video thumbnail that should of been generated does not exist.");
				return null;
			}
			int time = interval*i;
			if (i > 0) {
				time = (int) Math.round(time - ((double) interval/2));
			}
			videoThumbnails[i] = new VideoThumbnail(time, file);
		}
		return videoThumbnails;
	}
	
}
