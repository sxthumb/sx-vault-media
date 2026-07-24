use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum PipelineError {
    #[error("Erro de I/O: {0}")]
    Io(String),

    #[error("Operador '{operator_name}' falhou: {reason}")]
    OperatorFailed {
        operator_name: &'static str,
        reason: String,
    },

    #[error("Tentativas esgotadas para '{operator_name}' após {attempts} tentativas")]
    RetryExhausted {
        operator_name: &'static str,
        attempts: u32,
    },
}

impl From<std::io::Error> for PipelineError {
    fn from(err: std::io::Error) -> Self {
        PipelineError::Io(err.to_string())
    }
}