use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::operators::FnOperator;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::StreamOperator;

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

/// Fábrica — cria um validador de metadados de vídeo pronto para ser inserido na pipeline.
/// Lê o `VideoMetadata` diretamente do `PipelineContext` no momento do flush —
/// sem receber referências externas, sem `Arc<Mutex<T>>`.
pub fn validate_video_metadata() -> impl StreamOperator {
    FnOperator::new("validate_video_metadata").validate_it(|_state, ctx, _emitter| {
        let metadata =
            ctx.get::<VideoMetadata>().ok_or_else(|| PipelineError::OperatorFailed {
                operator_name: "validate_video_metadata",
                reason: "VideoMetadata não encontrado no contexto — \
                         verifique se 'extract_media_metadata' precede este operador"
                    .to_string(),
            })?;

        validate(metadata)
    })
}