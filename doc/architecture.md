# Arquitetura do SX Vault Media

## 1. Visão geral

O SX Vault Media é um serviço assíncrono em Rust voltado ao recebimento e
processamento de mídias (áudio, imagem e vídeo). A aplicação expõe uma API gRPC de upload em streaming,
processa os dados sem carregar o arquivo inteiro na memória (com consumo de memória constante $O(1)$) e devolve eventos
de progresso ao cliente.

A organização do código separa o domínio das tecnologias de transporte e
execução:

```text
Cliente gRPC
    │
    ▼
Adaptador inbound gRPC
    │  MediaProcessCommand + ByteStream
    ▼
Porta inbound do Core
    │
    ▼
UploadVideoUseCase (e processadores de mídias futuros)
    │
    ▼
Pipeline reativa de operadores
    │
    ├── Extração de metadados do conteúdo (extract_it)
    ├── Validação do conteúdo (validate_it)
    └── Eventos de progresso
            │
            ▼
      EventBus interno
            │
            ▼
      ProgressResponse gRPC
```

Uma explicação detalhada do funcionamento da arquitetura reativa com o ciclo de vida ETL de operators pode ser lida no [Wiki de Utilitários de Streaming](file:///e:/n2k6/projetos/sxthumb/sx-vault-media/doc/shared_utils_wiki.md).

---

## 2. Camadas e responsabilidades

### 2.1 Core

Localizado em `src/core`, o Core concentra os conceitos e os contratos do
processamento:

- `domain/types.rs`: `MediaMetadata` e `VideoMetadata`.
- `domain/entities/`: entidades específicas, atualmente com
  `video_metadata.rs`.
- `domain/errors/`: erros de domínio, como stream inválido, formato não
  suportado e falha de processamento.
- `ports/inbound/media_process.rs`: contrato de entrada
  `MediaProcessInbound`, além de `MediaProcessCommand`, `ByteStream` e
  `MediaProcessResult`.
- `use_cases/upload_video.rs`: implementação do caso de uso de upload de vídeo.
- `services/extract_media_metadata.rs`: serviço de extração construído sobre a engine reativa (`extract_it`) encarregado de identificar containers de mídia (áudio, imagem, vídeo) a partir dos bytes iniciais e atualizar o contexto.
- `services/validator_video.rs`: serviço de validação estruturado sobre `validate_it` que verifica as regras do vídeo a partir dos metadados extraídos no contexto.

### 2.2 Infraestrutura

Localizada em `src/infra`, contém os pontos de integração:

- `adapters/inbound/grpc/mod.rs`: implementa `MediaService`, converte a
  requisição gRPC para o contrato do Core e inicia o processamento.
- `adapters/inbound/grpc/stream.rs`: filtra eventos do barramento pelo ID da
  mídia e os converte em `ProgressResponse`.
- `runtime/`: inicialização de runtimes específicos.
- `config/`: configurações operacionais da aplicação.
- `adapters/outbound/`: reservado para armazenamento e demais dependências
  externas.

### 2.3 Shared

Localizada em `src/shared`, contém mecanismos reutilizáveis e utilitários técnicos agnósticos de domínio:

- `utils/streaming/context.rs`: `PipelineContext` (TypeMap) que permite tráfego de dados fortemente tipados de forma isolada e thread-safe entre operadores da pipeline.
- `utils/streaming/traits.rs`: contratos de operadores (`StreamOperator`), estados de etapa e emissor de progresso.
- `utils/streaming/pipe.rs`: motor da pipeline assíncrona; lê chunks de 16 KiB, repassa
  os dados pelos operadores, executa flush e trata erros.
- `utils/streaming/errors.rs`: `PipelineError` e conversões de erro de I/O.
- `utils/operators/builder.rs`: construtor de operadores fluente (`FnOperator`) baseado em closures para mapeamento de eventos de ciclo de vida.
- `utils/operators/extractor.rs`, `validator.rs`, `transformer.rs`, `loader.rs`: Extensões fluentes sob o `FnOperator` que provêem as assinaturas e comportamentos padrão para o pipeline de dados ETL.
- `utils/event_bus.rs`: barramento global baseado em
  `tokio::sync::broadcast`.
- `utils/grpc.rs`: conversão de `tonic::Streaming<T>` para `ByteStream`.

---

## 3. Fluxo de processamento e pipeline

1. O cliente envia uma sequência de `UploadChunkRequest`.
2. O controller gera um UUID para a mídia.
3. O campo `chunk_data` é extraído de cada mensagem e convertido para `Bytes`.
4. O controller monta um `MediaProcessCommand`.
5. Uma tarefa Tokio chama `MediaProcessInbound::process_media`.
6. O caso de uso compõe e executa o `StreamPipe`:
   ```rust
   let pipe = StreamPipe::with_operators(vec![
       Box::new(extract_media_metadata()),
       Box::new(validate_video_metadata()),
   ]);
   reactive_stream_pipe(reader, pipe, media_id).await
   ```
7. A pipeline lê o stream em blocos de 16 KiB, acumulando os bytes e rodando sequencialmente cada etapa.
8. O `extract_media_metadata` detecta assinaturas do cabeçalho da mídia (áudio, imagem ou vídeo) usando o mecanismo `extract_it` e popula o `PipelineContext` com os metadados.
9. O `validate_video_metadata` faz pass-through dos chunks e valida os metadados armazenados no `PipelineContext` durante o encerramento do fluxo (`validate_it`).
10. O `ProgressEmitter` publica eventos de status de processamento da mídia.

---

## 4. Contrato gRPC

O contrato está definido em `proto/media.proto`:

```protobuf
service MediaService {
    rpc UploadVideo (stream UploadChunkRequest)
        returns (stream ProgressResponse);
}
```

---

## 5. Tratamento de erros

- `PipelineError`: erros de I/O, falhas de operador ou exaustão de retentativas.
- `DomainError`: erros expostos pelo Core, contendo mensagens amigáveis de domínio para o cliente final.

Qualquer falha em operadores interrompe a pipeline e propaga o erro de forma graciosa.

---
