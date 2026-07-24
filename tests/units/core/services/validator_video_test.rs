use std::sync::{Arc, Mutex};
use sx_vault_media::core::domain::types::VideoMetadata;
use sx_vault_media::core::services::validator_video::validate_video_metadata;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};

#[tokio::test]
async fn test_validator_video_fails_on_invalid_mime() {
    let metadata = Arc::new(Mutex::new(VideoMetadata::default()));
    {
        let mut meta = metadata.lock().unwrap();
        meta.base.content_type = Some("image/png".to_string());
        meta.base.total_size_bytes = 100;
    }

    let validator = validate_video_metadata(metadata);
    let pipe = StreamPipe::with_operators(vec![Box::new(validator)]);

    let data = b"dummy_png_bytes";
    let res = reactive_stream_pipe(&data[..], pipe, "test_invalid_video".to_string()).await;
    assert!(res.is_err());
}
