use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::operators::FnOperator;
use crate::shared::utils::streaming::traits::{StepState, StreamOperator};

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

/// Fábrica — cria um operador de extração de metadados pronto para ser inserido na pipeline.
/// Usa `FnOperator` como base, com `do_it` (on_next) e `on_complete` (flush).
pub fn extract_media_metadata() -> impl StreamOperator {
    FnOperator::with_state("extract_media_metadata", ExtractorState::default())
        .do_it(|bytes, state, ctx, emitter| {
            // Garante que VideoMetadata já exista no contexto
            if ctx.get::<VideoMetadata>().is_none() {
                ctx.insert(VideoMetadata::default());
            }

            let metadata = ctx.get_mut::<VideoMetadata>().expect("inserido acima");

            // Contagem total de bytes do vídeo
            metadata.add_bytes(bytes.len() as u64);

            // Acumula os primeiros bytes para análise do cabeçalho (máx. 4096 bytes)
            if state.header_buffer.len() < 4096 {
                let needed = 4096 - state.header_buffer.len();
                let to_take = needed.min(bytes.len());
                state.header_buffer.extend_from_slice(&bytes[..to_take]);
            }

            // Tenta detectar o MIME type assim que tivermos dados suficientes
            // e o tipo ainda não tiver sido identificado
            if metadata.base.content_type.is_none() && !state.header_buffer.is_empty() {
                if let Some(kind) = infer::get(&state.header_buffer) {
                    metadata.base.content_type = Some(kind.mime_type().to_string());
                    emitter.emit(
                        StepState::Processing,
                        &format!("Container identificado: {}", kind.mime_type()),
                    );
                }
            }

            Ok(())
        })
        .on_complete(|state, ctx, emitter| {
            // Garante que o contexto tenha VideoMetadata mesmo para streams vazias
            if ctx.get::<VideoMetadata>().is_none() {
                ctx.insert(VideoMetadata::default());
            }

            let metadata = ctx.get_mut::<VideoMetadata>().expect("inserido acima");

            // Fallback quando o infer não reconheceu o formato
            if metadata.base.content_type.is_none() && !state.header_buffer.is_empty() {
                metadata.base.content_type = Some("application/octet-stream".to_string());
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
        })
}