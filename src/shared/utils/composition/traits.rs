use async_trait::async_trait;

use crate::shared::utils::streaming::errors::PipelineError;
use crate::shared::utils::streaming::traits::StreamOperator;

pub trait Validator<T>: StreamOperator {
    fn rule_name(&self) -> &'static str;
    fn validate(&self, target: &T) -> Result<(), PipelineError>;
}

#[async_trait]
pub trait Transformer<I, O>: Send + Sync {
    fn name(&self) -> &'static str;
    async fn transform(&self, input: I) -> Result<O, PipelineError>;
}

#[async_trait]
pub trait Loader<I, O>: Send + Sync {
    fn name(&self) -> &'static str;
    async fn load(&self, input: I) -> Result<O, PipelineError>;
}
