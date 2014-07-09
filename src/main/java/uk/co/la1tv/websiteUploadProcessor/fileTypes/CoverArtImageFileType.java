package uk.co.la1tv.websiteUploadProcessor.fileTypes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import uk.co.la1tv.websiteUploadProcessor.Config;
import uk.co.la1tv.websiteUploadProcessor.File;
import uk.co.la1tv.websiteUploadProcessor.helpers.DbHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.FileHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageMagickFileInfo;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageMagickFormat;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageMagickHelper;
import uk.co.la1tv.websiteUploadProcessor.helpers.ImageProcessorHelper;

public class CoverArtImageFileType extends FileTypeAbstract {
	
	public CoverArtImageFileType(int id) {
		super(id);
	}

	private static Logger logger = Logger.getLogger(CoverArtImageFileType.class);

	@Override
	public FileTypeProcessReturnInfo process(java.io.File source, java.io.File workingDir, File file) {
		// any image format and size is acceptable. It will always be cropped to become correct aspect ratio
		Config config = Config.getInstance();
		FileTypeProcessReturnInfo returnVal = new FileTypeProcessReturnInfo();
		// ids of files that should be marked in_use when the process_state is updated at the end of processing
		returnVal.fileIdsToMarkInUse = new HashSet<Integer>();
		List<Object> allFormats = config.getList("encoding.coverArtImageFormats");
		
		DbHelper.updateStatus(file.getId(), "Processing image.", null);
		
		ImageMagickFormat iMFormat = ImageMagickFormat.getFormatFromExtension(file.getExtension());
		if (iMFormat == null) {
			logger.warn("Error occurred when trying to get image magick formar from image file with id "+file.getId()+".");
			returnVal.msg = "Error trying to determine image format.";
			return returnVal;
		}
		
		final ArrayList<Format> formats = new ArrayList<Format>();
		for (Object f : allFormats) {
			String[] a = ((String) f).split("-");
			int w = Integer.parseInt(a[0]);
			int h = Integer.parseInt(a[1]);
			formats.add(new Format(w, h, new java.io.File(FileHelper.format(workingDir.getAbsolutePath()+"/output_"+w+"_"+h))));
		}
		
		for (Format f : formats) {
			logger.debug("Executing ImageMagick to process image file for source file with width "+f.w+" and height "+f.h+".");
			int exitVal = ImageProcessorHelper.process(iMFormat, ImageMagickFormat.JPG, source, f.outputFile, workingDir, f.w, f.h);
			if (exitVal != 0) {
				logger.warn("ImageMagick finished processing image but returned error code "+exitVal+".");
				returnVal.msg = "Error processing image.";
				return returnVal;
			}
		}

		DbHelper.updateStatus(file.getId(), "Finalizing.", null);
		Connection dbConnection = DbHelper.getMainDb().getConnection();
		ArrayList<OutputFile> outputFiles = new ArrayList<OutputFile>();
		
		try {
			for (Format f : formats) {
				
				long size = f.outputFile.length(); // size of file in bytes
				
				logger.debug("Creating file record for render with height "+f.h+" belonging to source file with id "+file.getId()+".");

				Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
				PreparedStatement s = dbConnection.prepareStatement("INSERT INTO files (in_use,created_at,updated_at,size,file_type_id,source_file_id,process_state) VALUES(0,?,?,?,?,?,1)", Statement.RETURN_GENERATED_KEYS);
				s.setTimestamp(1, currentTimestamp);
				s.setTimestamp(2, currentTimestamp);
				s.setLong(3, size);
				s.setInt(4, FileType.COVER_ART_IMAGE_RENDER.getObj().getId());
				s.setInt(5, file.getId());
				if (s.executeUpdate() != 1) {
					logger.warn("Error occurred when creating database entry for a file.");
					return returnVal;
				}
				ResultSet generatedKeys = s.getGeneratedKeys();
				generatedKeys.next();
				f.id = generatedKeys.getInt(1);
				logger.debug("File record created with id "+f.id+" for image render with width "+f.w+" and height "+f.h+" belonging to source file with id "+file.getId()+".");
				
				// add to set of files to mark in_use when processing completed
				returnVal.fileIdsToMarkInUse.add(f.id);
				
				// add entry to OutputFiles array which will be used to populate VideoFiles table later
				// get width and height of output
				
				ImageMagickFileInfo info = ImageMagickHelper.getFileInfo(iMFormat, f.outputFile, workingDir);
				if (info == null) {
					logger.warn("Error retrieving info for file rendered from source file with id "+file.getId()+".");
					return returnVal;
				}
				outputFiles.add(new OutputFile(f.id, info.getW(), info.getH()));
				
				// copy file to server
				logger.info("Moving output file with id "+f.id+" to web app...");
				if (!f.outputFile.renameTo(new java.io.File(FileHelper.format(config.getString("files.webappFilesLocation")+"/"+f.id)))) {
					logger.error("Error trying to move output file with id "+f.id+" to web app.");
					return returnVal;
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
				if (s.executeUpdate() != 1) {
					logger.debug("Error registering file with id "+o.id+" in image_files table.");
					return returnVal;
				}
				logger.debug("Created entry in image_files table for file with id "+o.id+".");
			}
			logger.debug("Created entries in image_files table.");
		} catch (SQLException e) {
			throw(new RuntimeException("Error trying to create entries in image_files."));
		}
		
		returnVal.success = true;
		return returnVal;
	}
	
	private class Format {
		
		public Format(int w, int h, java.io.File outputFile) {
			this.w = w;
			this.h = h;
			this.outputFile = outputFile;
		}
		
		public int id;
		public int w;
		public int h;
		public java.io.File outputFile;
	}
	
	private class OutputFile {
		
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
