use std::marker::PhantomData;

use async_trait::async_trait;

use super::traits::Loader;
use crate::shared::utils::streaming::errors::PipelineError;

pub struct FnLoader<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    name: &'static str,
    load_fn: F,
    _types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> FnLoader<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    pub fn new(name: &'static str, load_fn: F) -> Self {
        Self {
            name,
            load_fn,
            _types: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, F> Loader<I, O> for FnLoader<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, PipelineError> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    async fn load(&self, input: I) -> Result<O, PipelineError> {
        (self.load_fn)(input)
    }
}
