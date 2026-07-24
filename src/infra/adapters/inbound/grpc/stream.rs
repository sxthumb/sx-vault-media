use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

use crate::shared::utils::event_bus::{self, MediaEvent};
use super::proto::{ProgressResponse, UploadResponse};

pub type ProgressStream = std::pin::Pin<
    Box<dyn Stream<Item = Result<ProgressResponse, Status>> + Send + 'static>
>;

pub fn create_progress_stream(target_media_id: String) -> ProgressStream {
    let rx = event_bus::subscribe();

    let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
        Ok(event) => match event {
            MediaEvent::Progress { media_id, state, message, percentage }
                if media_id == target_media_id =>
            {
                Some(Ok(ProgressResponse {
                    id: media_id,
                    state,
                    message,
                    final_result: None,
                    percentage,
                }))
            }
            MediaEvent::Completed { media_id, vault_path, total_bytes } if media_id == target_media_id => {
                Some(Ok(ProgressResponse {
                    id: media_id.clone(),
                    state: "COMPLETED".to_string(),
                    message: format!("Processado com sucesso ({} bytes)", total_bytes),
                    final_result: Some(UploadResponse {
                        id: media_id,
                        vault_path,
                        success: true,
                    }),
                    percentage: 100.0,
                }))
            }
            MediaEvent::Failed { media_id, error } if media_id == target_media_id => {
                Some(Ok(ProgressResponse {
                    id: media_id,
                    state: "FAILED".to_string(),
                    message: error,
                    final_result: None,
                    percentage: 0.0,
                }))
            }
            _ => None,
        },
        Err(_) => None,
    });

    Box::pin(stream)
}