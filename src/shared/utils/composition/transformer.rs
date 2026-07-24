use std::marker::PhantomData;

use async_trait::async_trait;

use super::traits::Transformer;
use crate::shared::utils::streaming::errors::PipelineError;

pub struct FnTransformer<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    name: &'static str,
    transform_fn: F,
    _types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> FnTransformer<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    pub fn new(name: &'static str, transform_fn: F) -> Self {
        Self {
            name,
            transform_fn,
            _types: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, F> Transformer<I, O> for FnTransformer<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    async fn transform(&self, input: I) -> Result<O, PipelineError> {
        (self.transform_fn)(input)
    }
}
