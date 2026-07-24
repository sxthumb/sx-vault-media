use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::core::domain::errors::DomainError;
use crate::core::domain::types::VideoMetadata;
use crate::core::ports::inbound::media_process::{
    ByteStream, MediaProcessCommand, MediaProcessInbound, MediaProcessResult,
};
use crate::core::services::extract_media_metadata::extract_media_metadata;
use crate::core::services::validator_video::validate_video_metadata;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};

pub async fn upload_video<R>(
    reader: R,
    media_id: String,
    metadata: Arc<Mutex<VideoMetadata>>,
) -> Result<u64, PipelineError>
where
    R: AsyncRead + Unpin,
{
    let pipe = StreamPipe::with_operators(vec![
        Box::new(extract_media_metadata(Arc::clone(&metadata))),
        Box::new(validate_video_metadata(Arc::clone(&metadata))),
    ]);

    reactive_stream_pipe(reader, pipe, media_id).await
}

pub struct UploadVideoUseCase;

impl UploadVideoUseCase {
    pub fn new() -> Self {
        Self
    }
}

impl Default for UploadVideoUseCase {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MediaProcessInbound for UploadVideoUseCase {
    async fn process_media(
        &self,
        command: MediaProcessCommand,
        stream: ByteStream,
    ) -> Result<MediaProcessResult, DomainError> {
        let reader = StreamReader::new(stream);
        let metadata = Arc::new(Mutex::new(VideoMetadata::default()));

        let total_bytes = upload_video(reader, command.media_id.clone(), Arc::clone(&metadata))
            .await
            .map_err(|err| DomainError::PipelineProcessingFailed(err.to_string()))?;

        Ok(MediaProcessResult {
            media_id: command.media_id.clone(),
            vault_path: format!("/vault/storage/{}", command.media_id),
            total_bytes_processed: total_bytes,
            is_success: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_upload_video_success() {
        let dummy_mp4 = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2avc1mp41payload_data_here";
        let reader = &dummy_mp4[..];
        let metadata = Arc::new(Mutex::new(VideoMetadata::default()));

        let res = upload_video(reader, "vid_123".to_string(), Arc::clone(&metadata)).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), dummy_mp4.len() as u64);

        let final_meta = metadata.lock().unwrap();
        assert_eq!(final_meta.base.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(final_meta.base.total_size_bytes, dummy_mp4.len() as u64);
    }
}