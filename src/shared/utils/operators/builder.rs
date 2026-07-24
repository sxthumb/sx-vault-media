use async_trait::async_trait;
use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{ProgressEmitter, StreamOperator};

type NextFn<T> = Box<
    dyn FnMut(
            &[u8],
            &mut T,
            &mut PipelineContext,
            &dyn ProgressEmitter,
        ) -> Result<Option<Vec<u8>>, PipelineError>
        + Send
        + Sync,
>;
type FlushFn<T> = Box<
    dyn FnMut(
            &mut T,
            &mut PipelineContext,
            &dyn ProgressEmitter,
        ) -> Result<Option<Vec<u8>>, PipelineError>
        + Send
        + Sync,
>;
type ErrorFn<T> = Box<
    dyn FnMut(
            &PipelineError,
            &mut T,
            &mut PipelineContext,
            &dyn ProgressEmitter,
        ) -> Result<Option<Vec<u8>>, PipelineError>
        + Send
        + Sync,
>;

pub struct FnOperator<T = ()>
where
    T: Send + Sync + 'static,
{
    name: &'static str,
    pub(crate) state: T,
    on_next: Option<NextFn<T>>,
    on_flush: Option<FlushFn<T>>,
    on_error: Option<ErrorFn<T>>,
}

impl FnOperator<()> {
    pub fn new(name: &'static str) -> FnOperator<()> {
        FnOperator {
            name,
            state: (),
            on_next: None,
            on_flush: None,
            on_error: None,
        }
    }

    pub fn with_state<T: Send + Sync + 'static>(name: &'static str, state: T) -> FnOperator<T> {
        FnOperator {
            name,
            state,
            on_next: None,
            on_flush: None,
            on_error: None,
        }
    }
}

impl<T> FnOperator<T>
where
    T: Send + Sync + 'static,
{
    /// Registra a closure para `on_next` com controle total sobre o chunk de saída.
    pub fn on_next<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next = Some(Box::new(move |chunk, state, ctx, emitter| {
            f(chunk, state, ctx, emitter)
        }));
        self
    }


    /// Registra a closure para `on_complete` (flush final do stream).
    pub fn on_complete<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_flush = Some(Box::new(move |state, ctx, emitter| f(state, ctx, emitter)));
        self
    }

    /// Registra a closure para `on_error` (circuit breaker).
    pub fn on_error<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&PipelineError, &mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_error = Some(Box::new(move |err, state, ctx, emitter| {
            f(err, state, ctx, emitter)
        }));
        self
    }
}

#[async_trait]
impl<T> StreamOperator for FnOperator<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            Some(bytes) => {
                if let Some(ref mut next_fn) = self.on_next {
                    next_fn(bytes, &mut self.state, ctx, emitter)
                } else {
                    Ok(Some(bytes.to_vec()))
                }
            }
            None => {
                if let Some(ref mut flush_fn) = self.on_flush {
                    flush_fn(&mut self.state, ctx, emitter)
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn handle_error(
        &mut self,
        err: PipelineError,
        ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        if let Some(ref mut error_fn) = self.on_error {
            error_fn(&err, &mut self.state, ctx, emitter)
        } else {
            Err(err)
        }
    }
}