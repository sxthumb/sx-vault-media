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
use crate::shared::utils::streaming::pipe::reactive_stream_pipe;
use crate::shared::utils::composition::Validator;

pub async fn upload_video<R>(
    reader: R,
    media_id: String,
    metadata: Arc<Mutex<VideoMetadata>>,
) -> Result<u64, PipelineError>
where
    R: AsyncRead + Unpin,
{
    reactive_stream_pipe(
        reader,
        vec![
            Box::new(extract_media_metadata(metadata)),
            Box::new(validate_video_metadata(Arc::clone(&metadata))),
        ],
        media_id,
    )
    .await
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