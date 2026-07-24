use async_trait::async_trait;
use super::traits::Validator;
use crate::shared::utils::streaming::context::PipelineContext;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::{ProgressEmitter, StreamOperator};

/// Validator genérico baseado em closure. Recebe um `Arc<Mutex<T>>` como fonte de dados,
/// válido para usos onde o dado vem de fora do pipeline.
/// Para dados originados dentro do pipeline, prefira implementar `StreamOperator` diretamente
/// e ler do `PipelineContext`.
pub struct FnValidator<T, F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Result<(), PipelineError> + Send + Sync + 'static,
{
    rule_name: &'static str,
    target: std::sync::Arc<std::sync::Mutex<T>>,
    validate_fn: F,
}

impl<T, F> FnValidator<T, F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Result<(), PipelineError> + Send + Sync + 'static,
{
    pub fn new(
        rule_name: &'static str,
        target: std::sync::Arc<std::sync::Mutex<T>>,
        validate_fn: F,
    ) -> Self {
        Self {
            rule_name,
            target,
            validate_fn,
        }
    }
}

#[async_trait]
impl<T, F> StreamOperator for FnValidator<T, F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Result<(), PipelineError> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.rule_name
    }

    async fn process(
        &mut self,
        chunk: Option<&[u8]>,
        _ctx: &mut PipelineContext,
        _emitter: &dyn ProgressEmitter,
    ) -> Result<Option<Vec<u8>>, PipelineError> {
        match chunk {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => {
                let target = self.target.lock().map_err(|_| {
                    PipelineError::OperatorFailed {
                        operator_name: self.rule_name,
                        reason: "não foi possível acessar o valor para validação".to_string(),
                    }
                })?;

                self.validate(&target)?;
                Ok(None)
            }
        }
    }
}

impl<T, F> Validator<T> for FnValidator<T, F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Result<(), PipelineError> + Send + Sync + 'static,
{
    fn rule_name(&self) -> &'static str {
        self.rule_name
    }

    fn validate(&self, target: &T) -> Result<(), PipelineError> {
        (self.validate_fn)(target)
    }
}
