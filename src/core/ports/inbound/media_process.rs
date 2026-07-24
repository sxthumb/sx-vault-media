use std::pin::Pin;
use tokio_stream::Stream;
use bytes::Bytes;
use crate::core::domain::errors::DomainError;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

pub struct MediaProcessCommand {
    pub media_id: String,
    pub original_name: Option<String>,
    pub expected_content_type: Option<String>,
}

pub struct MediaProcessResult {
    pub media_id: String,
    pub vault_path: String,
    pub total_bytes_processed: u64,
    pub is_success: bool,
}

#[async_trait::async_trait]
pub trait MediaProcessInbound: Send + Sync {
    async fn process_media(
        &self,
        command: MediaProcessCommand,
        stream: ByteStream,
    ) -> Result<MediaProcessResult, DomainError>;
}