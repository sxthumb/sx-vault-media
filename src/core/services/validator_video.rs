use crate::core::domain::types::VideoMetadata;
use std::sync::{Arc, Mutex};

use crate::shared::utils::composition::validator::FnValidator;
use crate::shared::utils::composition::Validator;
use crate::shared::utils::streaming::errors::PipelineError;

const VIDEO_CONTENT_TYPES: &[&str] = &[
    "video/mp4",
    "video/webm",
    "video/quicktime",   // .mov (iPhones)
    "video/x-matroska",  // .mkv
    "video/x-msvideo",   // .avi
    "video/x-flv",       // .flv
    "video/ogg",         // .ogv / .ogg
    "video/3gpp",        // .3gp
];

pub fn validate_video_metadata(
    metadata: Arc<Mutex<VideoMetadata>>,
) -> impl Validator<VideoMetadata> {
    FnValidator::new(
        "validate_video_metadata",
        metadata,
        |metadata: &VideoMetadata| {
            // 1. Garante que o extractor conseguiu definir algum MIME type
            let raw_content_type = metadata
                .base
                .content_type
                .as_deref()
                .ok_or_else(|| PipelineError::OperatorFailed {
                    operator_name: "validate_video_metadata",
                    reason: "tipo de conteúdo não detectado".to_string(),
                })?;

            let content_type = raw_content_type.trim().to_lowercase();

            if !VIDEO_CONTENT_TYPES.iter().any(|&allowed| allowed == content_type) {
                return Err(PipelineError::OperatorFailed {
                    operator_name: "validate_video_metadata",
                    reason: format!("conteúdo não é um vídeo aceito: {}", content_type),
                });
            }

            if metadata.base.total_size_bytes == 0 {
                return Err(PipelineError::OperatorFailed {
                    operator_name: "validate_video_metadata",
                    reason: "vídeo sem conteúdo".to_string(),
                });
            }

            Ok(())
        },
    )
}