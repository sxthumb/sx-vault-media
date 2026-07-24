use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DomainError {
    UnsupportedFormat(String),
    InvalidStream(String),
    ValidationFailed(String),
    PipelineProcessingFailed(String),
    StorageFailed(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::UnsupportedFormat(msg) => write!(f, "Formato não suportado: {}", msg),
            DomainError::InvalidStream(msg) => write!(f, "Stream inválido: {}", msg),
            DomainError::ValidationFailed(msg) => write!(f, "Falha de validação: {}", msg),
            DomainError::PipelineProcessingFailed(msg) => write!(f, "Erro no pipeline: {}", msg),
            DomainError::StorageFailed(msg) => write!(f, "Falha no vault/armazenamento: {}", msg),
        }
    }
}

impl std::error::Error for DomainError {}