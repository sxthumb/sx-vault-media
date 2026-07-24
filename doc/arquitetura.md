# Arquitetura do SX Vault Media

## 1. Visão geral

O SX Vault Media é um serviço assíncrono em Rust voltado ao recebimento e
processamento de mídias. A aplicação expõe uma API gRPC de upload em streaming,
processa os dados sem carregar o arquivo inteiro na memória e devolve eventos
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
UploadVideoUseCase
    │
    ▼
Pipeline reativa de operadores
    │
    ├── Extração de metadados do conteúdo
    └── Eventos de progresso
            │
            ▼
      EventBus interno
            │
            ▼
      ProgressResponse gRPC
```

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
- `use_cases/upload_video.rs`: implementação do caso de uso de upload.
- `services/extract_media_metadata.rs`: operador responsável pela leitura
  inicial do conteúdo e identificação do contêiner.
- `services/validator_video.rs`: regra de validação que confirma se os
  metadados extraídos representam um tipo de vídeo aceito.

O caso de uso recebe um `ByteStream`, adapta-o para `AsyncRead` com
`StreamReader` e executa a pipeline. Ele não depende diretamente de Tonic ou
de objetos da camada de transporte.

### 2.2 Infraestrutura

Localizada em `src/infra`, contém os pontos de integração:

- `adapters/inbound/grpc/mod.rs`: implementa `MediaService`, converte a
  requisição gRPC para o contrato do Core e inicia o processamento.
- `adapters/inbound/grpc/stream.rs`: filtra eventos do barramento pelo ID da
  mídia e os converte em `ProgressResponse`.
- `runtime/`: reservado para inicialização de runtimes específicos.
- `config/`: reservado para configuração da aplicação.
- `adapters/outbound/`: reservado para armazenamento e demais dependências
  externas.

O servidor é inicializado em `src/main.rs`, no endereço `[::1]:50051`, com
limite máximo de decodificação de mensagem configurado em 2 GiB.

### 2.3 Shared

Localizada em `src/shared`, contém mecanismos técnicos que podem ser usados
por diferentes casos de uso:

- `utils/streaming/traits.rs`: contratos de operadores, estados de etapa e
  emissor de progresso.
- `utils/streaming/pipe.rs`: motor da pipeline; lê chunks de 16 KiB, repassa
  os dados pelos operadores, executa flush e trata erros.
- `utils/streaming/errors.rs`: `PipelineError` e conversões de erro de I/O.
- `utils/operators/builder.rs`: construtor de operadores baseados em closures,
  com estado próprio e callbacks de processamento, conclusão e erro.
- `utils/operators/extractor.rs`: adaptação de um `FnOperator` para a
  abstração `Extractor<T>`.
- `utils/composition/`: abstrações sobre valores processados, separadas do
  streaming de chunks:
  - `traits.rs`: `Validator<T>`, `Transformer<I, O>` e `Loader<I, O>`.
  - `validator.rs`: implementação `FnValidator`.
  - `transformer.rs`: implementação `FnTransformer`.
  - `loader.rs`: implementação `FnLoader`.
- `utils/event_bus.rs`: barramento global baseado em
  `tokio::sync::broadcast`.
- `utils/grpc.rs`: conversão de `tonic::Streaming<T>` para `ByteStream`.

## 3. Fluxo de upload

1. O cliente envia uma sequência de `UploadChunkRequest`.
2. O controller gera um UUID para a mídia.
3. O campo `chunk_data` é extraído de cada mensagem e convertido para
   `Bytes`.
4. O controller monta um `MediaProcessCommand`.
5. Uma tarefa Tokio chama `MediaProcessInbound::process_media`.
6. O caso de uso executa `upload_video`, que configura a pipeline com o
   operador `extract_media_metadata`.
7. A pipeline lê o stream em blocos de 16 KiB, contabiliza os bytes e executa
   cada operador.
8. O operador examina até 4096 bytes iniciais para identificar:
   - MP4, pelo marcador `ftyp`;
   - WebM, pelo cabeçalho EBML;
   - QuickTime, pelos marcadores `moov` ou `qt  `;
   - `application/octet-stream` como fallback.
9. O `ProgressEmitter` publica eventos com estado, mensagem e percentual.
10. Ao terminar, o caso de uso publica `Completed` ou `Failed`.
11. O stream de resposta converte o evento correspondente ao ID da mídia em
    `ProgressResponse`.

Após o flush do extractor, `upload_video` lê o metadata finalizado e executa
`validate_video_metadata` como parte da composição do caso de uso. O método
`process_media` apenas adapta o stream, delega para `upload_video` e monta o
resultado. Se a validação falhar, o erro é propagado como
`DomainError::PipelineProcessingFailed`; nenhuma etapa posterior é executada e
o controller publica o evento `Failed`.

## 4. Contrato gRPC

O contrato está definido em `proto/media.proto`:

```protobuf
service MediaService {
    rpc UploadVideo (stream UploadChunkRequest)
        returns (stream ProgressResponse);
}
```

### Entrada

`UploadChunkRequest` contém:

- `id`: identificador enviado pelo cliente, atualmente não usado para gerar o
  ID interno;
- `chunk_data`: bytes do chunk;
- `sequence_number`: número sequencial, atualmente transportado pelo contrato
  mas não validado pelo fluxo.

### Saída

`ProgressResponse` contém:

- `id`: ID interno da mídia;
- `state`: `STARTED`, `PROCESSING`, `COMPLETED` ou `FAILED`;
- `message`: descrição da etapa ou do erro;
- `percentage`: progresso estimado pela quantidade de etapas;
- `final_result`: preenchido somente no sucesso, com ID, caminho do vault e
  indicador de sucesso.

## 5. Modelo de progresso e eventos

O `EventBus` usa um canal broadcast com capacidade 1024. Os eventos são:

- `Progress`: emitido pelas etapas da pipeline;
- `Completed`: emitido pelo controller após o caso de uso concluir;
- `Failed`: emitido pelo controller quando o caso de uso retorna erro.

Cada stream de resposta se inscreve no barramento e descarta eventos de outras
mídias comparando o `media_id`. O progresso é calculado com base no número de
operadores configurados, não no percentual de bytes recebidos.

## 6. Tratamento de erros

Há dois níveis principais:

- `PipelineError`: erros de I/O, falha de operador e tentativas esgotadas.
- `DomainError`: erros expostos pelo Core, incluindo falhas de pipeline e
  armazenamento.

O operador pode tratar seu próprio erro por meio de `on_error`. Quando não há
tratamento configurado, o erro interrompe a pipeline e é convertido em
`DomainError::PipelineProcessingFailed` pelo caso de uso.

## 7. Estado atual versus intenção arquitetural

### Implementado

- API gRPC de upload em streaming.
- Conversão de chunks gRPC para fluxo assíncrono de bytes.
- Pipeline extensível baseada em `StreamOperator`.
- Estado por operador e callbacks de processamento, flush e erro.
- Extração inicial de tipo de contêiner.
- Eventos de progresso, sucesso e falha.
- Separação entre Core e adaptador gRPC por porta inbound.

### Em construção ou reservado

- Não há adaptador outbound implementado.
- O caminho do vault é calculado, mas não há gravação persistente do arquivo.
- `sequence_number` ainda não é validado.
- `original_name` e `expected_content_type` ainda não participam da validação.
- `config` e `runtime` ainda não possuem configuração operacional própria.
- A extração atual identifica o contêiner, mas ainda não preenche duração,
  resolução, FPS ou checksum.

## 8. Direção de evolução

Os próximos componentes naturais são:

1. Criar uma porta outbound para armazenamento de chunks e resultado.
2. Implementar o adaptador de filesystem ou storage escolhido para o vault.
3. Validar ordenação, duplicidade e integridade dos chunks.
4. Tornar endereço, limites e caminhos configuráveis.
5. Completar a extração de metadados e adicionar validações de domínio.
6. Adicionar testes para a pipeline, adaptadores e contrato de progresso.

## Evolução do Pipeline de Streaming: Padrão `PipelineContext`

O pipeline de reatividade em `shared/utils` é o coração da aplicação. Inicialmente, o compartilhamento de metadados entre operadores (como `Extractor` e `Validator`) dependia de instâncias explícitas de `Arc<Mutex<T>>` passadas manualmente via construtores no *client code*.

### Por que mudar?

1. **Violação do Princípio da Responsabilidade Única (SRP) e Acoplamento:** O código cliente precisava ter conhecimento das dependências internas dos operadores (ex: `validate_video_metadata(extractor.target())`), acoplando a montagem do pipeline ao estado de runtime.
2. **Evolução Fluida (Open/Closed Principle - OCP):** Para adicionar um novo operador que lê ou escreve dados acumulados (ex: métricas, auditoria, assinaturas), era necessário alterar a assinatura das funções de fábrica para passar novos `Arc<Mutex<T>>`.
3. **Preservação de Funções Puras / Imutabilidade I/O:** Cada etapa/operador no pipeline deve agir de forma previsível e isolada. Ao receber um contexto dinâmico de execução (`PipelineContext`), o operador lê apenas a fração de dados que precisa para processar a entrada ($I$) e produzir a saída ($O$), sem causar efeitos colaterais fora do escopo do pipeline.

### A Solução: PipelineContext

Com a introdução do `PipelineContext` repassado dinamicamente pelo `reactive_stream_pipe`:
- **Nenhum operador antigo quebra:** As traits abstratas (`Validator<T>`, `Extractor<T>`, `Transformer`, `Loader`) continuam sendo os contratos de domínio fortemente tipados.
- **Contexto de Execução "Caixa Preta":** O pipeline gerencia o ciclo de vida dos dados acumulados durante o streaming.
- **Composição Desacoplada:** Os operadores declarados no `StreamPipe` tornam-se completamente autônomos.