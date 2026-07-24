use sx_vault_media::core::use_cases::upload_video::upload_video;

#[tokio::test]
async fn test_upload_video_success() {
    let dummy_mp4 = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_data_here";
    let reader = &dummy_mp4[..];

    let res = upload_video(reader, "vid_123".to_string()).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), dummy_mp4.len() as u64);
}

#[tokio::test]
async fn test_upload_video_invalid_format_fails() {
    let dummy_text = b"this is not a valid video stream";
    let reader = &dummy_text[..];

    let res = upload_video(reader, "vid_invalid".to_string()).await;
    assert!(res.is_err());
}
