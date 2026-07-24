use std::sync::{Arc, Mutex};
use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::operators::builder::FnOperator;
use crate::shared::utils::operators::extractor::FnExtractor;
use crate::shared::utils::streaming::traits::{Extractor, StepState};

#[derive(Debug, Clone, Default)]
pub struct MediaMetadataExtractorState {
    pub metadata: VideoMetadata,
    pub header_buffer: Vec<u8>,
}

impl Extractor<VideoMetadata> for FnExtractor<MediaMetadataExtractorState> {
    fn extract(&self) -> &VideoMetadata {
        &self.inner.state.metadata
    }
}

pub fn extract_media_metadata(
    completed_metadata: Arc<Mutex<VideoMetadata>>,
) -> impl Extractor<VideoMetadata> {
    let metadata_for_complete = Arc::clone(&completed_metadata);

    let op = FnOperator::with_state("extract_media_metadata", MediaMetadataExtractorState::default())
        .do_it(move |chunk, state, emitter| {
            state.metadata.add_bytes(chunk.len() as u64);

            if state.header_buffer.len() < 4096 {
                let needed = 4096 - state.header_buffer.len();
                let to_take = needed.min(chunk.len());
                state.header_buffer.extend_from_slice(&chunk[..to_take]);

                if state.header_buffer.len() >= 12 && state.metadata.base.content_type.is_none() {
                    let buffer = &state.header_buffer;
                    if &buffer[4..8] == b"ftyp" {
                        state.metadata.base.content_type = Some("video/mp4".to_string());
                    } else if buffer.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
                        state.metadata.base.content_type = Some("video/webm".to_string());
                    } else if &buffer[4..8] == b"moov" || &buffer[4..8] == b"qt  " {
                        state.metadata.base.content_type = Some("video/quicktime".to_string());
                    }

                    if let Some(ref mime) = state.metadata.base.content_type {
                        emitter.emit(
                            StepState::Processing,
                            &format!("Container identificado: {}", mime),
                        );
                    }
                }
            }
            Ok(())
        })
        .on_error(|err, _state, emitter| {
            emitter.emit(
                StepState::Processing,
                &format!("Falha na extração de metadados: {}", err),
            );
            Err(err.clone())
        })
        .on_complete(move |state, emitter| {
            if state.metadata.base.content_type.is_none() && !state.header_buffer.is_empty() {
                state.metadata.base.content_type = Some("application/octet-stream".to_string());
            }

            let mime = state.metadata.base.content_type.as_deref().unwrap_or("desconhecido");
            emitter.emit(
                StepState::Completed,
                &format!("Metadados finalizados: {}", mime),
            );
            *metadata_for_complete.lock().unwrap() = state.metadata.clone();
            Ok(None)
        });

    FnExtractor::new(op)
}