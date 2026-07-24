use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{ProgressEmitter, StepState, StreamOperator};

/// Estado local do extrator — apenas o buffer de cabeçalho.
/// O `VideoMetadata` acumulado vive no `PipelineContext` para que outros
/// operadores posteriores na pipeline possam acessá-lo sem acoplamento direto.
struct ExtractorState {
    header_buffer: Vec<u8>,
}

impl Default for ExtractorState {
    fn default() -> Self {
        Self {
            header_buffer: Vec::with_capacity(4096),
        }
    }
}

pub struct MediaMetadataExtractor {
    state: ExtractorState,
}

impl MediaMetadataExtractor {
    pub fn new() -> Self {
        Self {
            state: ExtractorState::default(),
        }
    }
}

impl Default for MediaMetadataExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StreamOperator for MediaMetadataExtractor {
    fn name(&self) -> &'static str {
        "extract_media_metadata"
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            // ── Chunk recebido: acumular bytes e tentar inferir MIME ──────────
            Some(bytes) => {
                // Garante que VideoMetadata já exista no contexto
                if ctx.get::<VideoMetadata>().is_none() {
                    ctx.insert(VideoMetadata::default());
                }

                let metadata = ctx.get_mut::<VideoMetadata>().expect("inserido acima");

                // Contagem total de bytes do vídeo
                metadata.add_bytes(bytes.len() as u64);

                // Acumula os primeiros bytes para análise do cabeçalho (máx. 4096 bytes)
                if self.state.header_buffer.len() < 4096 {
                    let needed = 4096 - self.state.header_buffer.len();
                    let to_take = needed.min(bytes.len());
                    self.state
                        .header_buffer
                        .extend_from_slice(&bytes[..to_take]);
                }

                // Tenta detectar o MIME type assim que tivermos dados suficientes
                // e o tipo ainda não tiver sido identificado
                if metadata.base.content_type.is_none()
                    && !self.state.header_buffer.is_empty()
                {
                    if let Some(kind) = infer::get(&self.state.header_buffer) {
                        metadata.base.content_type = Some(kind.mime_type().to_string());
                        emitter.emit(
                            StepState::Processing,
                            &format!("Container identificado: {}", kind.mime_type()),
                        );
                    }
                }

                Ok(Some(bytes.to_vec()))
            }

            // ── Flush final: finalizar metadados no contexto ──────────────────
            None => {
                // Garante que o contexto tenha VideoMetadata mesmo para streams vazias
                if ctx.get::<VideoMetadata>().is_none() {
                    ctx.insert(VideoMetadata::default());
                }

                let metadata = ctx.get_mut::<VideoMetadata>().expect("inserido acima");

                // Fallback quando o infer não reconheceu o formato
                if metadata.base.content_type.is_none()
                    && !self.state.header_buffer.is_empty()
                {
                    metadata.base.content_type =
                        Some("application/octet-stream".to_string());
                }

                let mime = metadata
                    .base
                    .content_type
                    .as_deref()
                    .unwrap_or("desconhecido");

                emitter.emit(
                    StepState::Completed,
                    &format!("Metadados finalizados: {}", mime),
                );

                Ok(None)
            }
        }
    }

    async fn handle_error(
        &mut self,
        err: PipelineError,
        _ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        emitter.emit(
            StepState::Processing,
            &format!("Falha na extração de metadados: {}", err),
        );
        Err(err)
    }
}

/// Fábrica — cria um `MediaMetadataExtractor` pronto para ser inserido na pipeline.
pub fn extract_media_metadata() -> MediaMetadataExtractor {
    MediaMetadataExtractor::new()
}