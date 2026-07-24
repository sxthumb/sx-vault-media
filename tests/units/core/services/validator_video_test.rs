use sx_vault_media::core::domain::types::VideoMetadata;
use sx_vault_media::core::services::extract_media_metadata::extract_media_metadata;
use sx_vault_media::core::services::validator_video::validate_video_metadata;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};

#[tokio::test]
async fn test_validator_video_passes_for_valid_mp4() {
    // O validador depende do extrator para preencher o PipelineContext
    let mp4_data = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_content";
    let reader = &mp4_data[..];

    let pipe = StreamPipe::with_operators(vec![
        Box::new(extract_media_metadata()),
        Box::new(validate_video_metadata()),
    ]);

    let res = reactive_stream_pipe(reader, pipe, "test_valid_video".to_string()).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_validator_video_fails_on_invalid_mime() {
    use sx_vault_media::shared::utils::operators::FnOperator;
    use sx_vault_media::shared::utils::streaming::errors::PipelineError;

    // Injeta manualmente um VideoMetadata com MIME inválido no ctx
    // para simular um extrator que detectou um arquivo não-vídeo
    let setup = FnOperator::new("setup_ctx").on_complete(|_state, ctx, _emitter| {
        let mut meta = VideoMetadata::default();
        meta.base.content_type = Some("image/png".to_string());
        meta.base.total_size_bytes = 100;
        ctx.insert(meta);
        Ok(None)
    });

    let pipe = StreamPipe::with_operators(vec![
        Box::new(setup),
        Box::new(validate_video_metadata()),
    ]);

    let data = b"dummy_png_bytes";
    let res = reactive_stream_pipe(&data[..], pipe, "test_invalid_video".to_string()).await;
    assert!(res.is_err());

    match res.unwrap_err() {
        PipelineError::OperatorFailed { operator_name, reason } => {
            assert_eq!(operator_name, "validate_video_metadata");
            assert!(reason.contains("image/png"));
        }
        other => panic!("Erro inesperado: {:?}", other),
    }
}

#[tokio::test]
async fn test_validator_video_fails_when_no_context() {
    use sx_vault_media::shared::utils::streaming::errors::PipelineError;

    // Sem extrator — VideoMetadata ausente no ctx
    let pipe = StreamPipe::with_operators(vec![Box::new(validate_video_metadata())]);

    let data = b"some_data";
    let res = reactive_stream_pipe(&data[..], pipe, "test_no_ctx".to_string()).await;
    assert!(res.is_err());

    match res.unwrap_err() {
        PipelineError::OperatorFailed { operator_name, .. } => {
            assert_eq!(operator_name, "validate_video_metadata");
        }
        other => panic!("Erro inesperado: {:?}", other),
    }
}
