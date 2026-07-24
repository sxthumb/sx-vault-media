use sx_vault_media::core::domain::types::VideoMetadata;
use sx_vault_media::core::services::extract_media_metadata::extract_media_metadata;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};

#[tokio::test]
async fn test_extract_media_metadata_mp4_container() {
    let mp4_data = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_content";
    let reader = &mp4_data[..];

    let pipe = StreamPipe::with_operators(vec![Box::new(extract_media_metadata())]);

    let res = reactive_stream_pipe(reader, pipe, "media_extract_123".to_string()).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_extract_media_metadata_webm_container() {
    let webm_data = b"\x1A\x45\xDF\xA3\x99\x42\x86\x81\x01\x42\xF7\x81\x01payload";
    let reader = &webm_data[..];

    let pipe = StreamPipe::with_operators(vec![Box::new(extract_media_metadata())]);

    let res = reactive_stream_pipe(reader, pipe, "media_extract_webm".to_string()).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_extract_media_metadata_counts_bytes_correctly() {
    let mp4_data = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_content";

    // Roda o pipe e verifica os bytes via um segundo operador que lê do ctx
    use sx_vault_media::shared::utils::operators::FnOperator;
    use std::sync::{Arc, Mutex};

    let captured_bytes: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let captured_bytes_clone = captured_bytes.clone();

    let checker = FnOperator::new("checker").validate_it(move |_state, ctx, _emitter| {
        if let Some(meta) = ctx.get::<VideoMetadata>() {
            *captured_bytes_clone.lock().unwrap() = meta.base.total_size_bytes;
        }
        Ok(())
    });

    let pipe = StreamPipe::with_operators(vec![
        Box::new(extract_media_metadata()),
        Box::new(checker),
    ]);

    let res = reactive_stream_pipe(&mp4_data[..], pipe, "byte_count_test".to_string()).await;
    assert!(res.is_ok());
    assert_eq!(*captured_bytes.lock().unwrap(), mp4_data.len() as u64);
}
