use std::sync::{Arc, Mutex};
use crate::core::domain::types::VideoMetadata;
use crate::shared::utils::operators::builder::FnOperator;
use crate::shared::utils::operators::extractor::FnExtractor;
use crate::shared::utils::streaming::traits::{Extractor, StepState};

pub fn extract_media_metadata(
    completed_metadata: Arc<Mutex<VideoMetadata>>,
) -> impl Extractor<VideoMetadata> {
    let header_buffer = Arc::new(Mutex::new(Vec::with_capacity(4096)));

    let buf_for_tap = Arc::clone(&header_buffer);
    let buf_for_complete = Arc::clone(&header_buffer);
    let metadata_for_complete = Arc::clone(&completed_metadata);

    let op = FnOperator::with_state("extract_media_metadata", VideoMetadata::default())
        .do_it(move |chunk, metadata, emitter| {
            metadata.add_bytes(chunk.len() as u64);

            let mut buffer = buf_for_tap.lock().unwrap();

            if buffer.len() < 4096 {
                let needed = 4096 - buffer.len();
                let to_take = needed.min(chunk.len());
                buffer.extend_from_slice(&chunk[..to_take]);

                // Checa se o tipo já não foi detectado anteriormente
                if buffer.len() >= 12 && metadata.base.content_type.is_none() {
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
        .on_error(|err, _metadata, emitter| {
            emitter.emit(
                StepState::Processing,
                &format!("Falha na extração de metadados: {}", err),
            );
            Err(err.clone())
        })
        .on_complete(move |metadata, emitter| {
            let buffer = buf_for_complete.lock().unwrap();

            if metadata.base.content_type.is_none() && !buffer.is_empty() {
                metadata.base.content_type = Some("application/octet-stream".to_string());
            }

            let mime = metadata.base.content_type.as_deref().unwrap_or("desconhecido");
            emitter.emit(
                StepState::Completed,
                &format!("Metadados finalizados: {}", mime),
            );
            *metadata_for_complete.lock().unwrap() = metadata.clone();
            Ok(None)
        });

    FnExtractor::new(op)
}