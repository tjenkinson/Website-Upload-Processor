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
		
		exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("ffmpeg.probeLocation"), "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", file.getAbsolutePath()}, workingDir, streamMonitor, null);
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
		
		try {
			metadata = new JSONObject(streamMonitor.getOutput());
			w = metadata.getJSONArray("streams").getJSONObject(0).getInt("width");
			h = metadata.getJSONArray("streams").getJSONObject(0).getInt("height");
			Fraction f = new FractionFormat().parse(metadata.getJSONArray("streams").getJSONObject(0).getString("r_frame_rate"));
			frameRate = f.doubleValue();
			duration = Double.parseDouble(metadata.getJSONObject("format").getString("duration"));
			noFrames = Double.parseDouble(metadata.getJSONArray("streams").getJSONObject(0).getString("nb_frames"));
		}
		catch(JSONException e) {
			logger.warn("Error parsing JSON from ffprobe for file '"+file.getAbsolutePath()+"'.");
			e.printStackTrace();
			return null;
		}
		
		return new FfmpegFileInfo(w, h, frameRate, duration, noFrames);
	}
	
}
