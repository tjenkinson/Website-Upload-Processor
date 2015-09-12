package uk.co.la1tv.websiteUploadProcessor.fileTypes;

public enum FileType {
	SIDE_BANNERS_IMAGE(new SideBannersImageFileType(1)),
	COVER_IMAGE(new CoverImageFileType(2)),
	VOD_VIDEO(new VODVideoFileType(3)),
	COVER_ART_IMAGE(new CoverArtImageFileType(4)),
	SIDE_BANNERS_IMAGE_RENDER(new SideBannersImageRenderFileType(5)),
	COVER_IMAGE_RENDER(new CoverImageRenderFileType(6)),
	VOD_VIDEO_RENDER(new VODVideoRenderFileType(7)),
	COVER_ART_IMAGE_RENDER(new CoverArtImageRenderFileType(8)),
	VOD_SCRUB_THUMBNAIL(new VODScrubThumbnailFileType(9)),
	SIDE_BANNERS_FILL_IMAGE(new SideBannersFillImageFileType(10)),
	SIDE_BANNERS_FILL_IMAGE_RENDER(new SideBannersFillImageRenderFileType(11)),
	DASH_MEDIA_PRESENTATION_DESCRIPTION(new DashMediaPresentationDescriptionType(12)),
	DASH_SEGMENT(new DashSegmentType(13)),
	HLS_MASTER_PLAYLIST(new HlsMasterPlaylistType(14)),
	HLS_MEDIA_PLAYLIST(new HlsMediaPlaylistType(15)),
	HLS_SEGMENT(new HlsSegmentType(16));
	
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
