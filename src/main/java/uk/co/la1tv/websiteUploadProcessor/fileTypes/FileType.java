package uk.co.la1tv.websiteUploadProcessor.fileTypes;

public enum FileType {
	SIDE_BANNER_IMAGES(new SideBannersImageFileType(1)),
	COVER_IMAGE(new CoverImageFileType(2)),
	VOD_VIDEO(new VODVideoFileType(3)),
	COVER_ART_IMAGE(new CoverArtImageFileType(4)),
	SIDE_BANNER_IMAGES_RENDER(new SideBannersImageRenderFileType(5)),
	COVER_IMAGE_RENDER(new CoverImageRenderFileType(6)),
	VOD_VIDEO_RENDER(new VODVideoRenderFileType(7)),
	COVER_ART_IMAGE_RENDER(new CoverArtImageRenderFileType(8));
	
	private final FileTypeAbstract instance;
	
	private FileType(FileTypeAbstract a) {
		instance = a;
	}
	
	public FileTypeAbstract getObj() {
		return instance;
	}
	
	/**
	 * Get a FileType object (object extending FileTypeAbstract) corresponding to the id.
	 * @param id
	 * @return Something extending FileTypeAbstract which represents a file type.
	 */
	public static FileTypeAbstract getFromId(int id) {
		for (FileType a : FileType.values()) {
			if (a.getObj().getId() == id) {
				return a.getObj();
			}
		}
		throw(new RuntimeException("File type object could not be found for id "+id+"."));
	}
}
