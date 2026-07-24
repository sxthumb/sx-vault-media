use async_trait::async_trait;
use super::builder::FnOperator;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{Extractor, ProgressEmitter, StreamOperator};

pub struct FnExtractor<T>
where
    T: Send + Sync + 'static,
{
    inner: FnOperator<T>,
}

impl<T> FnExtractor<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(operator: FnOperator<T>) -> Self {
        Self { inner: operator }
    }
}

#[async_trait]
impl<T> StreamOperator for FnExtractor<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        self.inner.process(chunk, emitter).await
    }

    async fn handle_error(
        &mut self,
        err: PipelineError,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        self.inner.handle_error(err, emitter).await
    }
}

impl<T> Extractor<T> for FnExtractor<T>
where
    T: Send + Sync + 'static,
{
    fn extract(&self) -> &T {
        &self.inner.state
    }
}