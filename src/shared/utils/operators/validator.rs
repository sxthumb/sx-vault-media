use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::ProgressEmitter;
use super::builder::FnOperator;

impl<T> FnOperator<T>
where
    T: Send + Sync + 'static,
{
    /// Configura o operador como **Validador**:
    /// - `on_next`: pass-through automático do chunk
    /// - `on_complete`: executa a closure de validação lendo do `PipelineContext`
    ///
    /// A closure recebe `(&mut T, &mut PipelineContext, &dyn ProgressEmitter)`
    /// e retorna `Result<(), PipelineError>`. Se retornar `Err`, a pipeline é interrompida.
    pub fn validate_it<F>(self, mut f: F) -> Self
    where
        F: FnMut(&mut T, &mut PipelineContext, &dyn ProgressEmitter) -> Result<(), PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next(|chunk, _state, _ctx, _emitter| Ok(Some(chunk.to_vec())))
            .on_complete(move |state, ctx, emitter| {
                f(state, ctx, emitter)?;
                Ok(None)
            })
    }
}
