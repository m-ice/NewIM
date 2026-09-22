use newim_sdk_core::media::{
    MAX_MEDIA_PAYLOAD_BYTES, MAX_MEDIA_SIZE, MediaError, MediaKind, MediaMetadata,
    valid_content_type, valid_media_key,
};

fn metadata() -> MediaMetadata {
    MediaMetadata {
        media_key: "media_01".into(),
        kind: MediaKind::Image,
        content_type: "image/jpeg".into(),
        size: 12,
        sha256: [7; 32],
    }
}

#[test]
fn media_metadata_is_closed_and_bounded() {
    let metadata = metadata();
    assert!(metadata.validate().is_ok());
    assert!(!metadata.has_binary_field());
    assert_eq!(MediaKind::parse("video"), Ok(MediaKind::Video));
    assert_eq!(
        MediaKind::parse("application"),
        Err(MediaError::InvalidInput)
    );
    assert_eq!(MediaKind::Image.as_str(), "image");
    assert_eq!(MAX_MEDIA_PAYLOAD_BYTES, 4096);
    assert_eq!(MAX_MEDIA_SIZE, 104_857_600);
}

#[test]
fn media_metadata_rejects_non_matching_boundaries() {
    assert!(valid_media_key("A_b-9"));
    assert!(!valid_media_key(""));
    assert!(!valid_media_key("a/b"));
    let first = "a".repeat(63);
    let second = "b".repeat(64);
    assert!(valid_content_type(&format!("{first}/{second}")));
    assert!(!valid_content_type(&format!(
        "{}/{}",
        "a".repeat(64),
        second
    )));
    assert!(!valid_content_type(&format!(
        "{}/{}",
        first,
        "b".repeat(65)
    )));
    assert!(!valid_content_type("Image/JPEG"));
    assert!(!valid_content_type("image/jpeg; charset=utf-8"));

    let mut invalid = metadata();
    invalid.size = 0;
    assert_eq!(invalid.validate(), Err(MediaError::InvalidInput));
    invalid.size = MAX_MEDIA_SIZE + 1;
    assert_eq!(invalid.validate(), Err(MediaError::InvalidInput));
    invalid.size = 12;
    invalid.kind = MediaKind::File;
    assert!(invalid.validate().is_ok());
}
