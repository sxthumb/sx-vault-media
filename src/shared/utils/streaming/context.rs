use std::any::{Any, TypeId};
use std::collections::HashMap;

/// Contexto mutável compartilhado ao longo da vida de uma stream.
/// Permite que operadores armazenem e recuperem estado tipado sem acoplamento direto entre si.
/// Gerenciado pelo `reactive_stream_pipe` — operadores não precisam instanciá-lo.
pub struct PipelineContext {
    store: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl PipelineContext {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Insere (ou substitui) um valor tipado no contexto.
    pub fn insert<T: 'static + Send + Sync>(&mut self, val: T) {
        self.store.insert(TypeId::of::<T>(), Box::new(val));
    }

    /// Retorna uma referência imutável ao valor tipado, se presente.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.store
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Retorna uma referência mutável ao valor tipado, se presente.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.store
            .get_mut(&TypeId::of::<T>())
            .and_then(|v| v.downcast_mut::<T>())
    }
}

impl Default for PipelineContext {
    fn default() -> Self {
        Self::new()
    }
}
