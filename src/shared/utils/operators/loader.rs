use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::ProgressEmitter;
use super::builder::FnOperator;

impl<T> FnOperator<T>
where
    T: Send + Sync + 'static,
{
    /// Configura o operador como **Loader**:
    /// - `on_next`: grava os bytes no destino final (S3, disco, socket, etc.)
    ///   e repassa o chunk adiante para permitir encadeamento de loaders.
    ///
    /// A closure recebe `(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)`
    /// e retorna `Result<(), PipelineError>`. Use `.on_complete(...)` em seguida
    /// para confirmar o upload/commit após o fim do stream.
    pub fn load_it<F>(self, mut f: F) -> Self
    where
        F: FnMut(&[u8], &mut T, &mut PipelineContext, &dyn ProgressEmitter)
                -> Result<(), PipelineError>
            + Send
            + Sync
            + 'static,
    {
        self.on_next(move |chunk, state, ctx, emitter| {
            f(chunk, state, ctx, emitter)?;
            Ok(Some(chunk.to_vec()))
        })
    }
}
