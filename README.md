# SX Vault Media

Serviço Rust para receber mídias por streaming gRPC, processar o conteúdo em
chunks e publicar o progresso do processamento. O projeto é a base do
serviço de armazenamento e análise de vídeos do ecossistema SX.

## O que o projeto faz hoje

O fluxo implementado é:

1. O cliente abre um upload bidirecional em streaming pelo método
   `MediaService.UploadVideo`.
2. O adaptador gRPC transforma cada `UploadChunkRequest` em um fluxo de bytes.
3. Um identificador UUID é criado para a mídia e o caso de uso de upload é
   executado de forma assíncrona.
4. A pipeline lê o conteúdo em chunks de 16 KiB e executa os operadores
   configurados.
5. O operador atual acumula o tamanho recebido e tenta identificar contêineres
   MP4, WebM e QuickTime a partir do cabeçalho.
6. Eventos de progresso, conclusão ou falha são publicados em um barramento
   interno e expostos ao cliente como `ProgressResponse`.

> A persistência definitiva no vault ainda não está implementada. O resultado
> atual retorna um caminho calculado no formato
> `/vault/storage/{media_id}`.

## Arquitetura

O código segue uma separação inspirada em Arquitetura Hexagonal/Clean
Architecture:

- **Core**: domínio, portas, casos de uso e serviços de negócio.
- **Infra**: adaptadores de entrada, runtime e configuração.
- **Shared**: abstrações reutilizáveis para streaming, operadores, eventos e
  conversão de streams gRPC.

A descrição detalhada, o fluxo de execução, o contrato gRPC e o estado atual
dos componentes estão em [`doc/arquitetura.md`](doc/arquitetura.md).

## Estrutura do repositório

```text
.
├── proto/
│   └── media.proto             # Contrato gRPC
├── src/
│   ├── main.rs                 # Inicialização do servidor
│   ├── lib.rs                  # Módulos públicos da crate
│   ├── build.rs                # Geração do código protobuf
│   ├── core/                   # Domínio e regras de negócio
│   │   ├── domain/
│   │   ├── ports/
│   │   ├── services/
│   │   └── use_cases/
│   ├── infra/                  # Integrações com o mundo externo
│   │   ├── adapters/
│   │   │   ├── inbound/grpc/
│   │   │   └── outbound/
│   │   ├── config/
│   │   └── runtime/
│   └── shared/                 # Componentes técnicos compartilhados
│       └── utils/
│           ├── event_bus.rs
│           ├── grpc.rs
│           ├── operators/
│           ├── composition/        # Abstrações sobre valores processados
└── streaming/
├── Cargo.toml
└── doc/
    └── arquitetura.md
```

## Tecnologias

- Rust 2024
- Tokio para runtime assíncrono
- Tonic, Prost e Protocol Buffers para gRPC
- Serde para serialização dos tipos de domínio
- `tokio::sync::broadcast` para eventos internos de progresso

As abstrações reutilizáveis de composição estão em
`src/shared/utils/composition/`:

- `Validator<T>` valida um valor sem transformá-lo.
- `Transformer<I, O>` transforma um valor em outro.
- `Loader<I, O>` persiste ou publica um valor e retorna o resultado da operação.

## Executando localmente

Pré-requisitos:

- Rust e Cargo instalados

Para compilar:

```bash
cargo check
```

Para iniciar o servidor:

```bash
cargo run
```

O servidor escuta, por padrão, em `[::1]:50051`.

## Contrato gRPC

O contrato fonte está em [`proto/media.proto`](proto/media.proto). O método
disponível atualmente é:

```text
rpc UploadVideo (stream UploadChunkRequest)
    returns (stream ProgressResponse)
```

O código Rust correspondente é gerado durante o build por `src/build.rs`; os
artefatos gerados não devem ser editados manualmente.

## Estado atual e próximos passos

- A entrada gRPC e a pipeline de chunks estão conectadas.
- A extração inicial de tipo de contêiner está implementada.
- O barramento interno envia eventos de progresso e resultado.
- O adaptador de saída e a gravação física no vault ainda são pontos de
  extensão.
- Validações de formato, checksum, metadados completos e configuração externa
  ainda podem ser adicionados.
