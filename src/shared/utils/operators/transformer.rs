use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::ProgressEmitter;
use super::builder::FnOperator;

impl<T> FnOperator<T>
where
    T: Send + Sync + 'static,
{
    /// Configura o operador como **Transformador**:
    /// - `on_next`: recebe o chunk, retorna o chunk **modificado** (ex: criptografado, comprimido)
    ///
    /// A closure recebe `(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)`
    /// e retorna `Result<Vec<u8>, PipelineError>` — os novos bytes que seguirão para o próximo operador.
    pub fn transform_it<F>(self, mut f: F) -> Self
    where
        F: FnMut(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<Vec<u8>, PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next(move |chunk, state, ctx, emitter| {
            let new_chunk = f(chunk, state, ctx, emitter)?;
            Ok(Some(new_chunk))
        })
    }
}
