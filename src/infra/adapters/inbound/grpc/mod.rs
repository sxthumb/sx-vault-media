use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::core::ports::inbound::media_process::{MediaProcessCommand, MediaProcessInbound};
use crate::shared::utils::event_bus::{self, MediaEvent};
use crate::shared::utils::grpc::tonic_stream_to_byte_stream;

pub mod proto {
    tonic::include_proto!("media");
}

pub mod stream;

pub use proto::media_service_server::{MediaService, MediaServiceServer};
use proto::UploadChunkRequest;
pub use stream::{create_progress_stream, ProgressStream};

pub struct MediaGrpcController {
    upload_use_case: Arc<dyn MediaProcessInbound>,
}

impl MediaGrpcController {
    pub fn new(upload_use_case: Arc<dyn MediaProcessInbound>) -> Self {
        Self { upload_use_case }
    }
}

#[tonic::async_trait]
impl MediaService for MediaGrpcController {
    type UploadVideoStream = ProgressStream;

    async fn upload_video(
        &self,
        request: Request<Streaming<UploadChunkRequest>>,
    ) -> Result<Response<Self::UploadVideoStream>, Status> {
        let media_id = Uuid::new_v4().to_string();

        let byte_stream = tonic_stream_to_byte_stream(request.into_inner(), |req| req.chunk_data);

        let command = MediaProcessCommand {
            media_id: media_id.clone(),
            original_name: None,
            expected_content_type: None,
        };

        let target_id = media_id.clone();
        let use_case = Arc::clone(&self.upload_use_case);
        let progress_stream = create_progress_stream(media_id);

        tokio::spawn(async move {
            match use_case.process_media(command, byte_stream).await {
                Ok(res) => {
                    event_bus::publish(MediaEvent::Completed {
                        media_id: target_id,
                        total_bytes: res.total_bytes_processed,
                    });
                }
                Err(err) => {
                    event_bus::publish(MediaEvent::Failed {
                        media_id: target_id,
                        error: err.to_string(),
                    });
                }
            }
        });

        Ok(Response::new(progress_stream))
    }
}