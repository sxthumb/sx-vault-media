use super::errors::PipelineError;
use super::traits::{ProgressEmitter, StepState, StreamOperator};
use crate::shared::utils::event_bus::{self, MediaEvent};
use tokio::io::{AsyncRead, AsyncReadExt};

struct ContextualEmitter<'a> {
    step_index: usize,
    total_steps: usize,
    media_id: &'a str,
}

impl<'a> ProgressEmitter for ContextualEmitter<'a> {
    fn emit(&self, state: StepState, message: &str) {
        let completed_steps = match &state {
            StepState::Completed => self.step_index + 1,
            StepState::Started | StepState::Processing => self.step_index,
        };
        let percentage = (completed_steps as f32 / self.total_steps as f32) * 100.0;

        event_bus::publish(MediaEvent::Progress {
            media_id: self.media_id.to_string(),
            state: state.to_string(),
            message: message.to_string(),
            percentage,
        });
    }
}

pub async fn reactive_stream_pipe<R>(
    mut reader: R,
    mut operators: Vec<Box<dyn StreamOperator>>,
    media_id: String,
) -> Result<u64, PipelineError>
where
    R: AsyncRead + Unpin,
{
    let total_steps = operators.len();
    let mut buffer = [0u8; 16384];
    let mut total_bytes_lidos: u64 = 0;

    // 1. Notifica o início das etapas na pipeline
    for (idx, op) in operators.iter().enumerate() {
        let emitter = ContextualEmitter {
            step_index: idx,
            total_steps,
            media_id: &media_id,
        };
        emitter.emit(StepState::Started, &format!("Iniciando etapa '{}'", op.name()));
    }

    // 2. Loop de leitura e streaming por chunks
    loop {
        let bytes_lidos = reader.read(&mut buffer).await?;
        if bytes_lidos == 0 {
            break;
        }

        total_bytes_lidos += bytes_lidos as u64;
        let mut current_data = Some(buffer[..bytes_lidos].to_vec());

        for (idx, op) in operators.iter_mut().enumerate() {
            let emitter = ContextualEmitter {
                step_index: idx,
                total_steps,
                media_id: &media_id,
            };

            if let Some(input) = current_data {
                match op.process(Some(&input), &emitter).await {
                    Ok(output) => current_data = output,
                    Err(err) => {
                        // Delega ao tratamento de erro oficial do operador repassando o emitter
                        return op.handle_error(err, &emitter).await.map(|_| total_bytes_lidos);
                    }
                }
            } else {
                break;
            }
        }
    }

    // 3. Flush final e encerramento dos operadores
    for (idx, op) in operators.iter_mut().enumerate() {
        let emitter = ContextualEmitter {
            step_index: idx,
            total_steps,
            media_id: &media_id,
        };

        if let Err(err) = op.process(None, &emitter).await {
            return op.handle_error(err, &emitter).await.map(|_| total_bytes_lidos);
        }

        // Emite a conclusão bem-sucedida do operador
        emitter.emit(
            StepState::Completed,
            &format!("Etapa '{}' finalizada com sucesso", op.name()),
        );
    }

    Ok(total_bytes_lidos)
}