use async_trait::async_trait;
use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{ProgressEmitter, StreamOperator};

type NextFn<T> = Box<
    dyn FnMut(&[u8], &mut T, &dyn ProgressEmitter) -> Result<Option<Vec<u8>>, PipelineError>
        + Send
        + Sync,
>;
type FlushFn<T> = Box<
    dyn FnMut(&mut T, &dyn ProgressEmitter) -> Result<Option<Vec<u8>>, PipelineError>
        + Send
        + Sync,
>;
type ErrorFn<T> = Box<
    dyn FnMut(
            &PipelineError,
            &mut T,
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
    pub fn on_next<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&[u8], &mut T, &dyn ProgressEmitter) -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next = Some(Box::new(move |chunk, state, emitter| {
            f(chunk, state, emitter)
        }));
        self
    }

    /// Variante simplificada: a closure retorna `Result<(), _>` e o chunk é passado adiante automaticamente.
    pub fn do_it<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&[u8], &mut T, &dyn ProgressEmitter) -> Result<(), PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next = Some(Box::new(move |chunk, state, emitter| {
            f(chunk, state, emitter)?;
            Ok(Some(chunk.to_vec()))
        }));
        self
    }

    pub fn on_complete<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut T, &dyn ProgressEmitter) -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_flush = Some(Box::new(move |state, emitter| f(state, emitter)));
        self
    }

    pub fn on_error<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(
                &PipelineError,
                &mut T,
                &dyn ProgressEmitter,
            ) -> Result<Option<Vec<u8>>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_error = Some(Box::new(move |err, state, emitter| {
            f(err, state, emitter)
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

    /// O `PipelineContext` é recebido mas não repassado às closures — a API das closures
    /// não muda. Operadores que precisam de ctx devem implementar `StreamOperator` diretamente.
    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        _ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            Some(bytes) => {
                if let Some(ref mut next_fn) = self.on_next {
                    next_fn(bytes, &mut self.state, emitter)
                } else {
                    Ok(Some(bytes.to_vec()))
                }
            }
            None => {
                if let Some(ref mut flush_fn) = self.on_flush {
                    flush_fn(&mut self.state, emitter)
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn handle_error(
        &mut self,
        err: PipelineError,
        _ctx: &mut PipelineContext,
        emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        if let Some(ref mut error_fn) = self.on_error {
            error_fn(&err, &mut self.state, emitter)
        } else {
            Err(err)
        }
    }
}