use sx_vault_media::core::services::extract_media_metadata::extract_media_metadata;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};

#[tokio::test]
async fn test_extract_media_metadata_mp4_container() {
    let mp4_data = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_content";
    let reader = &mp4_data[..];

    let extractor = extract_media_metadata();
    let target = extractor.target();

    let pipe = StreamPipe::with_operators(vec![Box::new(extractor)]);

    let res = reactive_stream_pipe(reader, pipe, "media_extract_123".to_string()).await;
    assert!(res.is_ok());

    let meta = target.lock().unwrap();
    assert_eq!(meta.base.content_type.as_deref(), Some("video/mp4"));
    assert_eq!(meta.base.total_size_bytes, mp4_data.len() as u64);
}

#[tokio::test]
async fn test_extract_media_metadata_webm_container() {
    let webm_data = b"\x1A\x45\xDF\xA3\x99\x42\x86\x81\x01\x42\xF7\x81\x01payload";
    let reader = &webm_data[..];

    let extractor = extract_media_metadata();
    let target = extractor.target();

    let pipe = StreamPipe::with_operators(vec![Box::new(extractor)]);

    let res = reactive_stream_pipe(reader, pipe, "media_extract_webm".to_string()).await;
    assert!(res.is_ok());

    let meta = target.lock().unwrap();
    assert_eq!(meta.base.content_type.as_deref(), Some("video/webm"));
}
