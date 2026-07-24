use std::sync::{Arc, Mutex};
use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::operators::builder::FnOperator;
use crate::shared::utils::operators::extractor::FnExtractor;
use crate::shared::utils::streaming::traits::{Extractor, StepState};

#[derive(Debug, Clone)]
pub struct MediaMetadataExtractorState {
    pub target: Arc<Mutex<VideoMetadata>>,
    pub header_buffer: Vec<u8>,
}

impl Default for MediaMetadataExtractorState {
    fn default() -> Self {
        Self {
            target: Arc::new(Mutex::new(VideoMetadata::default())),
            header_buffer: Vec::with_capacity(4096),
        }
    }
}

pub struct MediaMetadataExtractor {
    inner: FnExtractor<MediaMetadataExtractorState>,
}

impl MediaMetadataExtractor {
    pub fn new() -> Self {
        let op = FnOperator::with_state("extract_media_metadata", MediaMetadataExtractorState::default())
            .do_it(move |chunk, state, emitter| {
                let mut metadata = state.target.lock().map_err(|_| {
                    crate::shared::utils::streaming::errors::PipelineError::OperatorFailed {
                        operator_name: "extract_media_metadata",
                        reason: "falha ao adquirir lock de metadados".to_string(),
                    }
                })?;

                metadata.add_bytes(chunk.len() as u64);

                if state.header_buffer.len() < 4096 {
                    let needed = 4096 - state.header_buffer.len();
                    let to_take = needed.min(chunk.len());
                    state.header_buffer.extend_from_slice(&chunk[..to_take]);

                    if state.header_buffer.len() >= 12 && metadata.base.content_type.is_none() {
                        let buffer = &state.header_buffer;
                        if &buffer[4..8] == b"ftyp" {
                            metadata.base.content_type = Some("video/mp4".to_string());
                        } else if buffer.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
                            metadata.base.content_type = Some("video/webm".to_string());
                        } else if &buffer[4..8] == b"moov" || &buffer[4..8] == b"qt  " {
                            metadata.base.content_type = Some("video/quicktime".to_string());
                        }

                        if let Some(ref mime) = metadata.base.content_type {
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
                let mut metadata = state.target.lock().map_err(|_| {
                    crate::shared::utils::streaming::errors::PipelineError::OperatorFailed {
                        operator_name: "extract_media_metadata",
                        reason: "falha ao adquirir lock de metadados no flush".to_string(),
                    }
                })?;

                if metadata.base.content_type.is_none() && !state.header_buffer.is_empty() {
                    metadata.base.content_type = Some("application/octet-stream".to_string());
                }

                let mime = metadata.base.content_type.as_deref().unwrap_or("desconhecido");
                emitter.emit(
                    StepState::Completed,
                    &format!("Metadados finalizados: {}", mime),
                );
                Ok(None)
            });

        Self {
            inner: FnExtractor::new(op),
        }
    }

    pub fn target(&self) -> Arc<Mutex<VideoMetadata>> {
        Arc::clone(&self.inner.state().target)
    }

    pub fn get_metadata(&self) -> VideoMetadata {
        self.inner.state().target.lock().unwrap().clone()
    }
}

impl Default for MediaMetadataExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl crate::shared::utils::streaming::traits::StreamOperator for MediaMetadataExtractor {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        emitter: &dyn crate::shared::utils::streaming::traits::ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, crate::shared::utils::streaming::errors::PipelineError> {
        self.inner.process(chunk, emitter).await
    }

    async fn handle_error(
        &mut self,
        err: crate::shared::utils::streaming::errors::PipelineError,
        emitter: &dyn crate::shared::utils::streaming::traits::ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, crate::shared::utils::streaming::errors::PipelineError> {
        self.inner.handle_error(err, emitter).await
    }
}

impl Extractor<Arc<Mutex<VideoMetadata>>> for MediaMetadataExtractor {
    fn extract(&self) -> &Arc<Mutex<VideoMetadata>> {
        &self.inner.state().target
    }
}

pub fn extract_media_metadata() -> MediaMetadataExtractor {
    MediaMetadataExtractor::new()
}