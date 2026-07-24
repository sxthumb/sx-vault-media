use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

use crate::shared::utils::event_bus::{self, MediaEvent};
use super::proto::{ProgressResponse};

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
                    is_success: None,
                    percentage,
                }))
            }
            MediaEvent::Completed { media_id, total_bytes } if media_id == target_media_id => {
                Some(Ok(ProgressResponse {
                    id: media_id.clone(),
                    state: "COMPLETED".to_string(),
                    message: format!("Processado com sucesso ({} bytes)", total_bytes),
                    is_success: Some(true),
                    percentage: 100.0,
                }))
            }
            MediaEvent::Failed { media_id, error } if media_id == target_media_id => {
                Some(Ok(ProgressResponse {
                    id: media_id,
                    state: "FAILED".to_string(),
                    message: error,
                    is_success: Some(false),
                    percentage: 0.0,
                }))
            }
            _ => None,
        },
        Err(_) => None,
    });

    Box::pin(stream)
}