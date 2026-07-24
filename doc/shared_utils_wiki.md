# Wiki: Utilitários Compartilhados de Streaming (shared/utils)

Este documento descreve a arquitetura reativa de streaming baseada em pipeline desenvolvida sob a pasta `src/shared/utils`, bem como as diretrizes e exemplos de utilização dos builders e operadores.

---

## 1. Visão Geral e Arquitetura

O ecossistema de streaming foi desenhado para processar fluxos de dados de forma assíncrona com consumo de memória constante $O(1)$, independentemente do tamanho do arquivo original. 

A comunicação entre os operadores de uma pipeline ocorre de maneira desacoplada e segura através de um contexto de armazenamento heterogêneo compartilhado (`PipelineContext`), eliminando o acoplamento direto de dependências e a necessidade de travas manuais complexas (`Arc<Mutex<T>>`).

### A Pipeline e o Fluxo de Dados

```
                      ┌───────────────────────────────┐
                      │    reactive_stream_pipe(...)  │ (Orquestrador)
                      └───────────────┬───────────────┘
                                      │ Instancia & Injeta
                                      ▼
                             ┌─────────────────┐
                             │ PipelineContext │ (Heterogeneous Store)
                             └────────┬────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
     │   Extractor     │     │   Validator     │     │     Loader      │
     │  (extract_it)   │     │  (validate_it)  │     │    (load_it)    │
     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## 2. A Abstração Core: `FnOperator` e ciclo de vida ETL

Todos os operadores implementam a trait `StreamOperator`. Em vez de codificar structs manuais para cada operador, utilizamos o `FnOperator` que provê métodos fluentes para especializar o comportamento baseado no ciclo de vida de streams:

1. **`extract_it(on_chunk, on_flush)`**:
   - `on_chunk`: Executado a cada chunk lido. Analisa os bytes (ex: assinatura, contagem de tamanho) e os salva no `PipelineContext`. O chunk é repassado intacto adiante de forma automática.
   - `on_flush`: Executado ao fim do stream. Consolida e fecha os metadados finais.

2. **`validate_it(on_complete)`**:
   - Não interfere no fluxo dos bytes durante o streaming (pass-through automático).
   - `on_complete`: Lê o estado acumulado no `PipelineContext` e valida as regras de negócio. Caso falhe, retorna um erro e interrompe a pipeline.

3. **`transform_it(on_chunk)`**:
   - Modifica os bytes de cada chunk (ex: encriptação, compressão) e retorna o novo chunk modificado para os operadores seguintes.

4. **`load_it(on_chunk)`**:
   - Grava os bytes recebidos no destino de persistência (ex: Multipart upload para o S3 ou escrita em disco local).

---

## 3. Como Criar um Novo Operador (Serviço)

Qualquer serviço fora do diretório `shared/utils` deve apenas compor comportamentos usando os helpers expostos pelo `FnOperator`.

### Exemplo 1: Validador de Metadados
Lê o `VideoMetadata` preenchido anteriormente no contexto e garante a conformidade do arquivo.
```rust
use crate::shared::utils::operators::FnOperator;
use crate::shared::utils::streaming::traits::StreamOperator;
use crate::shared::utils::streaming::errors::PipelineError;
use crate::core::domain::types::VideoMetadata;

pub fn validate_video_metadata() -> impl StreamOperator {
    FnOperator::new("validate_video_metadata")
        .validate_it(|_state, ctx, _emitter| {
            let metadata = ctx.get::<VideoMetadata>()
                .ok_or_else(|| PipelineError::OperatorFailed {
                    operator_name: "validate_video_metadata",
                    reason: "Metadados ausentes no contexto".to_string(),
                })?;

            if metadata.base.total_size_bytes == 0 {
                return Err(PipelineError::OperatorFailed {
                    operator_name: "validate_video_metadata",
                    reason: "O arquivo está vazio".to_string(),
                });
            }
            Ok(())
        })
}
```

### Exemplo 2: Transformador de Criptografia (Pass-through modificado)
Modifica cada chunk individualmente em tempo de execução.
```rust
pub fn encrypt_chunks_transformer() -> impl StreamOperator {
    FnOperator::new("encrypt_chunks")
        .transform_it(|chunk, _state, _ctx, _emitter| {
            // Lógica fictícia de encriptação de bytes
            let encrypted = chunk.iter().map(|b| b ^ 0x5A).collect::<Vec<u8>>();
            Ok(encrypted)
        })
}
```

---

## 4. Orquestrando na Prática (Use Cases)

No seu caso de uso (ex: `upload_video.rs`), a orquestração resume-se a compor os operadores de forma fluente e declarativa dentro de uma `StreamPipe`:

```rust
use crate::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};
use crate::core::services::extract_media_metadata::extract_media_metadata;
use crate::core::services::validator_video::validate_video_metadata;

pub async fn upload_video<R>(reader: R, media_id: String) -> Result<u64, PipelineError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    // Apenas listamos as instâncias em caixas (Box) na ordem de execução
    let pipe = StreamPipe::with_operators(vec![
        Box::new(extract_media_metadata()),
        Box::new(validate_video_metadata()),
        // Box::new(seu_novo_operador_aqui()),
    ]);

    // Executa reativamente
    reactive_stream_pipe(reader, pipe, media_id).await
}
```
