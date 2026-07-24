use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::ProgressEmitter;
use super::builder::FnOperator;

impl<T> FnOperator<T>
where
    T: Send + Sync + 'static,
{
    /// Configura o operador como **Extrator**:
    /// - `on_next`: processa cada chunk (ex: detecta MIME, acumula header, atualiza ctx)
    ///   e repassa o chunk adiante automaticamente.
    /// - `on_complete`: finaliza a extração e grava o objeto completo no `PipelineContext`.
    ///
    /// Ambas as closures recebem `(&mut T, &mut PipelineContext, &dyn ProgressEmitter)`
    /// e retornam `Result<(), PipelineError>` — o roteamento do chunk é gerenciado
    /// internamente pelo `FnOperator`.
    pub fn extract_it<FN, FC>(self, mut on_chunk: FN, mut on_flush: FC) -> Self
    where
        FN: FnMut(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<(), PipelineError>
            + Send
            + Sync
            + 'static,
        FC: FnMut(&mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<(), PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next(move |chunk, state, ctx, emitter| {
                on_chunk(chunk, state, ctx, emitter)?;
                Ok(Some(chunk.to_vec()))
            })
            .on_complete(move |state, ctx, emitter| {
                on_flush(state, ctx, emitter)?;
                Ok(None)
            })
    }
}
