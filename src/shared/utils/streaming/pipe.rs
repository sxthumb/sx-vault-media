use async_trait::async_trait;
use super::errors::PipelineError;
use super::traits::{ProgressEmitter, StepState, StreamOperator};
use crate::shared::utils::event_bus::{self, MediaEvent};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct StreamPipe {
    name: &'static str,
    pub(crate) operators: Vec<Box<dyn StreamOperator>>,
}

pub type Pipe = StreamPipe;

impl StreamPipe {
    pub fn new(name: &'static str, operators: Vec<Box<dyn StreamOperator>>) -> Self {
        Self { name, operators }
    }

    pub fn with_operators(operators: Vec<Box<dyn StreamOperator>>) -> Self {
        Self::new("stream_pipe", operators)
    }

    pub fn add_operator(&mut self, operator: Box<dyn StreamOperator>) {
        self.operators.push(operator);
    }

    pub fn operators(&self) -> &[Box<dyn StreamOperator>] {
        &self.operators
    }

    pub fn operators_mut(&mut self) -> &mut [Box<dyn StreamOperator>] {
        &mut self.operators
    }
}

impl From<Vec<Box<dyn StreamOperator>>> for StreamPipe {
    fn from(operators: Vec<Box<dyn StreamOperator>>) -> Self {
        Self::with_operators(operators)
    }
}

#[async_trait]
impl StreamOperator for StreamPipe {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            Some(bytes) => {
                let mut current_data = Some(bytes.to_vec());
                for op in self.operators.iter_mut() {
                    if let Some(ref input) = current_data {
                        match op.process(Some(input), emitter).await {
                            Ok(output) => current_data = output,
                            Err(err) => {
                                return op.handle_error(err, emitter).await;
                            }
                        }
                    } else {
                        break;
                    }
                }
                Ok(current_data)
            }
            None => {
                for op in self.operators.iter_mut() {
                    if let Err(err) = op.process(None, emitter).await {
                        return op.handle_error(err, emitter).await;
                    }
                }
                Ok(None)
            }
        }
    }

    async fn handle_error(
        &mut self,
        err: PipelineError,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        for op in self.operators.iter_mut() {
            if let Ok(res) = op.handle_error(err.clone(), emitter).await {
                return Ok(res);
            }
        }
        Err(err)
    }
}

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

pub async fn reactive_stream_pipe<R, P>(
    mut reader: R,
    pipe: P,
    media_id: String,
) -> Result<u64, PipelineError>
where
    R: AsyncRead + Unpin,
    P: Into<StreamPipe>,
{
    let mut pipe = pipe.into();
    let total_steps = pipe.operators().len();
    let mut buffer = [0u8; 16384];
    let mut total_bytes_lidos: u64 = 0;

    // 1. Notifica o início das etapas na pipeline
    for (idx, op) in pipe.operators().iter().enumerate() {
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

        for (idx, op) in pipe.operators_mut().iter_mut().enumerate() {
            let emitter = ContextualEmitter {
                step_index: idx,
                total_steps,
                media_id: &media_id,
            };

            if let Some(input) = current_data {
                match op.process(Some(&input), &emitter).await {
                    Ok(output) => current_data = output,
                    Err(err) => {
                        return op.handle_error(err, &emitter).await.map(|_| total_bytes_lidos);
                    }
                }
            } else {
                break;
            }
        }
    }

    // 3. Flush final e encerramento dos operadores
    for (idx, op) in pipe.operators_mut().iter_mut().enumerate() {
        let emitter = ContextualEmitter {
            step_index: idx,
            total_steps,
            media_id: &media_id,
        };

        if let Err(err) = op.process(None, &emitter).await {
            return op.handle_error(err, &emitter).await.map(|_| total_bytes_lidos);
        }

        emitter.emit(
            StepState::Completed,
            &format!("Etapa '{}' finalizada com sucesso", op.name()),
        );
    }

    Ok(total_bytes_lidos)
}