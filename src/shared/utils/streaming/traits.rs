use async_trait::async_trait;
use super::context::PipelineContext;
use super::errors::PipelineError;

#[derive(Debug, Clone)]
pub enum StepState {
    Started,
    Processing,
    Completed,
}

impl std::fmt::Display for StepState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Started => "STARTED",
            Self::Processing => "PROCESSING",
            Self::Completed => "COMPLETED",
        };

        formatter.write_str(state)
    }
}

pub trait ProgressEmitter: Send + Sync {
    fn emit(&self, state: StepState, message: &str);
}

pub struct NoOpEmitter;

impl ProgressEmitter for NoOpEmitter {
    fn emit(&self, _state: StepState, _message: &str) {}
}

#[async_trait]
pub trait StreamOperator: Send + Sync {
    fn name(&self) -> &'static str;

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError>;

    async fn handle_error(
        &mut self,
        err: PipelineError,
        _ctx: &mut PipelineContext,
        _emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        Err(err)
    }
}

