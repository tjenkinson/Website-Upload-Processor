package uk.co.la1tv.websiteUploadProcessor.helpers;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileType;
import uk.co.la1tv.websiteUploadProcessor.fileTypes.FileTypeProcessReturnInfo;

public class ImageProcessorHelper {

	private static Logger logger = Logger.getLogger(ImageProcessorHelper.class);
	
	/**
	 * Scale the image source file passed in so that it fills a box of with w and height h and then crops and writes to destination.
	 * @param inputFormat: the ImageMagick format of the input file. ("identify -list format" to get supported formats)
	 * @param outputFormat: the ImageMagick format of the output file.
	 * @param source
	 * @param destination
	 * @param workingDir
	 * @param w
	 * @param h
	 * @return exit val from ImageMagick
	 */
	public static int renderImage(ImageMagickFormat inputFormat, ImageMagickFormat outputFormat, java.io.File source, java.io.File destination, java.io.File workingDir, int w, int h) {
		Config config = Config.getInstance();
		int exitVal = RuntimeHelper.executeProgram(new String[] {config.getString("imagemagick.convertLocation"), inputFormat.getIMFormat()+":"+source.getAbsolutePath(), "-resize", w+"x"+h+"^", "-gravity", "center", "-crop", w+"x"+h+"+0+0", outputFormat.getIMFormat()+":"+destination.getAbsolutePath()}, workingDir, null, null);
		return exitVal;
	}
	
	public static List<ImageFormat> getFormats(List<Object> allFormats, java.io.File workingDir) {
		final ArrayList<ImageFormat> formats = new ArrayList<ImageFormat>();
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int w = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			formats.add(new ImageFormat(w, h, new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/output_"+w+"_"+h))));
		}
		return formats;
	}
	
	/**
	 * Processes the source image to all of the formats in the formats array and copies the files across to the web app. Also updates the process status in the database. 
	 * @param dbConnection: A Connection object providing a connection to the database.
	 * @param returnVal: The FileTypeProcessReturnInfo object which will eventually be returned to process() in File.
	 * @param source: The source file.
	 * @param workingDir: The working directory.
	 * @param formats: The formats array that the source file will get processed into.
	 * @param inputFormat: The Image Magick format of the input file.
	 * @param outputFormat: The image Magick format of the output file.
	 * @param outputFileType: The file type that the output file will be.
	 * @param file: The File object associated with the input file.
	 * @return True if the processing is successful or false if it failed.
	 */
	public static boolean process(Connection dbConnection, FileTypeProcessReturnInfo returnVal, java.io.File source, java.io.File workingDir, List<ImageFormat> formats, ImageMagickFormat inputFormat, ImageMagickFormat outputFormat, File file, FileType outputFileType) {
		
		BigInteger totalSize = BigInteger.ZERO;
		
		for (ImageFormat f : formats) {
			logger.debug("Executing ImageMagick to process image file for source file with width "+f.w+" and height "+f.h+".");
			int exitVal = ImageProcessorHelper.renderImage(inputFormat, outputFormat, source, f.outputFile, workingDir, f.w, f.h);
			if (exitVal != 0) {
				logger.warn("ImageMagick finished processing image but returned error code "+exitVal+".");
				returnVal.msg = "Error processing image.";
				return false;
			}
			totalSize.add(new BigInteger(""+f.outputFile.length()));
			if (FileHelper.isOverQuota(totalSize)) {
				returnVal.msg = "Ran out of space.";
				return false;
			}
		}

		DbHelper.updateStatus(dbConnection, file.getId(), "Finalizing.", null);
		ArrayList<OutputFile> outputFiles = new ArrayList<OutputFile>();
		
		try {
			for (ImageFormat f : formats) {
				
				long size = f.outputFile.length(); // size of file in bytes
				
				logger.debug("Creating file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");

				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,process_state) VALUES(0,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
				s.setTimestamp(1, currentTimestamp);
				s.setTimestamp(2, currentTimestamp);
				s.setLong(3, size);
				s.setInt(4, outputFileType.getObj().getId());
				s.setInt(5, file.getId());
				if (s.executeUpdate() != 1) {
					s.close();
					logger.warn("Error occurred when creating database entry for a file.");
					return false;
				}
				ResultSet generatedKeys = s.getGeneratedKeys();
				generatedKeys.next();
				f.id = generatedKeys.getInt(1);
				s.close();
				logger.debug("File record created with id "+f.id+" for image render with width "+f.w+" and height "+f.h+" belonging to source file with id "+file.getId()+".");
				
				// add to set of files to mark in_use when processing completed
				returnVal.fileIdsToMarkInUse.add(f.id);
				
				// add entry to OutputFiles array which will be used to populate VideoFiles table later
				// get width and height of output
				
				ImageMagickFileInfo info = ImageMagickHelper.getFileInfo(inputFormat, f.outputFile, workingDir);
				if (info == null) {
					logger.warn("Error retrieving info for file rendered from source file with id "+file.getId()+".");
					return false;
				}
				
				outputFiles.add(new OutputFile(f.id, info.getW(), info.getH()));
				
				// copy file to server
				logger.info("Moving output file with id "+f.id+" to web app...");
				if (!FileHelper.moveToWebApp(f.outputFile, f.id)) {
					logger.error("Error trying to move output file with id "+f.id+" to web app.");
					return false;
				}
				logger.info("Output file with id "+f.id+" moved to web app.");
			}
		} catch (SQLException e) {
			throw(new RuntimeException("Error trying to register files in database."));
		}
		
		
		try {
			// create entries in image_files
			logger.debug("Creating entries in image_files table...");
			for (OutputFile o : outputFiles) {
				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO image_files (width,height,created_at,updated_at,file_id) VALUES (?,?,?,?,?)");
				s.setInt(1, o.w);
				s.setInt(2, o.h);
				s.setTimestamp(3, currentTimestamp);
				s.setTimestamp(4, currentTimestamp);
				s.setInt(5, o.id);
				int result = s.executeUpdate();
				s.close();
				if (result != 1) {
					logger.debug("Error registering file with id "+o.id+" in image_files table.");
					return false;
				}
				logger.debug("Created entry in image_files table for file with id "+o.id+".");
			}
			logger.debug("Created entries in image_files table.");
		} catch (SQLException e) {
			throw(new RuntimeException("Error trying to create entries in image_files."));
		}
		
		return true;
	}
	
	private static class OutputFile {
		
		public int id;
		public int w;
		public int h;
		
		public OutputFile(int id, int w, int h) {
			this.id = id;
			this.w = w;
			this.h = h;
		}
	}	
}
