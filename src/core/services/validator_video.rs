use async_trait::async_trait;

use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{ProgressEmitter, StreamOperator};

const VIDEO_CONTENT_TYPES: &[&str] = &[
    "video/mp4",
    "video/webm",
    "video/quicktime",  // .mov (iPhones)
    "video/x-matroska", // .mkv
    "video/x-msvideo",  // .avi
    "video/x-flv",      // .flv
    "video/ogg",        // .ogv / .ogg
    "video/3gpp",       // .3gp
];

/// Valida os metadados do vídeo extraídos pelo `MediaMetadataExtractor`.
/// Lê o `VideoMetadata` diretamente do `PipelineContext` no momento do flush —
/// sem receber referências externas, sem `Arc<Mutex<T>>`.
pub struct VideoMetadataValidator;

impl VideoMetadataValidator {
    pub fn new() -> Self {
        Self
    }

    fn validate(metadata: &VideoMetadata) -> Result<(), PipelineError> {
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

        if !VIDEO_CONTENT_TYPES
            .iter()
            .any(|&allowed| allowed == content_type)
        {
            return Err(PipelineError::OperatorFailed {
                operator_name: "validate_video_metadata",
                reason: format!("conteúdo não é um vídeo aceito: {}", content_type),
            });
        }

        // 2. Garante que o arquivo não está vazio
        if metadata.base.total_size_bytes == 0 {
            return Err(PipelineError::OperatorFailed {
                operator_name: "validate_video_metadata",
                reason: "vídeo sem conteúdo".to_string(),
            });
        }

        Ok(())
    }
}

impl Default for VideoMetadataValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StreamOperator for VideoMetadataValidator {
    fn name(&self) -> &'static str {
        "validate_video_metadata"
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        ctx: &mut PipelineContext,
        _emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            // Durante o streaming: transparente — apenas passa os bytes adiante
            Some(bytes) => Ok(Some(bytes.to_vec())),

            // Flush final: lê VideoMetadata do contexto e valida
            None => {
                let metadata =
                    ctx.get::<VideoMetadata>().ok_or_else(|| PipelineError::OperatorFailed {
                        operator_name: "validate_video_metadata",
                        reason: "VideoMetadata não encontrado no contexto — \
                                 verifique se 'extract_media_metadata' precede este operador"
                            .to_string(),
                    })?;

                Self::validate(metadata)?;
                Ok(None)
            }
        }
    }
}

/// Fábrica — cria um `VideoMetadataValidator` pronto para ser inserido na pipeline.
/// Não recebe parâmetros: os metadados são lidos do `PipelineContext` em tempo de execução.
pub fn validate_video_metadata() -> VideoMetadataValidator {
    VideoMetadataValidator::new()
}