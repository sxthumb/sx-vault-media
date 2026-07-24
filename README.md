# SX Vault Media

Serviço Rust para receber mídias (áudio, imagem e vídeo) por streaming gRPC, processar o conteúdo em chunks sob consumo de memória constante $O(1)$ e publicar o progresso do processamento. O projeto serve como infraestrutura base de armazenamento e ingestão de mídias no ecossistema SX.

---

## Documentação Técnica
- 📝 **[Arquitetura do Projeto](file:///e:/n2k6/projetos/sxthumb/sx-vault-media/doc/arquitetura.md)**: Detalhamento de camadas, fluxos gRPC e responsabilidades.
- 📖 **[Wiki de Utilitários de Streaming (shared/utils)](file:///e:/n2k6/projetos/sxthumb/sx-vault-media/doc/shared_utils_wiki.md)**: Explicações da arquitetura reativa, ciclo de vida ETL (`extract_it`, `validate_it`, `transform_it`, `load_it`) e exemplos de criação de operadores.

---

## O que o projeto faz hoje

O fluxo implementado é:

1. O cliente abre um upload bidirecional em streaming pelo método `MediaService.UploadVideo`.
2. O adaptador gRPC transforma cada `UploadChunkRequest` em um fluxo assíncrono de bytes.
3. Um identificador UUID é criado para a mídia e o caso de uso de upload é executado de forma assíncrona.
4. A pipeline reativa lê o conteúdo em chunks de 16 KiB e executa as etapas configuradas.
5. O extrator (`extract_media_metadata`) acumula os bytes e tenta identificar contêineres de mídia (áudio, imagem ou vídeo) a partir do cabeçalho inicial dos dados, populando o `PipelineContext`.
6. O validador (`validate_video_metadata`) lê o contexto e executa as regras de validação associadas.
7. Eventos de progresso, conclusão ou falha são publicados em um barramento interno e expostos ao cliente como `ProgressResponse`.

> A persistência definitiva no vault físico ainda não está implementada. O resultado atual retorna um caminho fictício no formato `/vault/storage/{media_id}`.

---

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
│   │   ├── services/           # Serviços baseados nos helpers do FnOperator
│   │   └── use_cases/
│   ├── infra/                  # Adaptadores de Entrada/Saída, runtime e config
│   │   └── adapters/
│   └── shared/                 # Componentes técnicos utilitários
│       └── utils/
│           ├── event_bus.rs
│           ├── grpc.rs
│           ├── streaming/      # Engine de pipeline, PipelineContext e erros
│           └── operators/      # FnOperator e helpers ETL (extract_it, validate_it, etc.)
└── doc/
    ├── arquitetura.md          # Detalhamento de arquitetura
    └── shared_utils_wiki.md    # Guia do desenvolvedor para a pipeline reativa
```

---

## Tecnologias

- Rust 2024
- Tokio para runtime assíncrono
- Tonic, Prost e Protocol Buffers para gRPC
- Serde para serialização dos tipos de domínio
- `tokio::sync::broadcast` para eventos internos de progresso
- `infer` para detecção de MIME types a partir do fluxo de bytes

---

## Executando localmente

Pré-requisitos:
- Rust e Cargo instalados

Para compilar:
```bash
cargo check
```

Para rodar a suite de testes unitários:
```bash
cargo test
```

Para iniciar o servidor gRPC local:
```bash
cargo run
```
O servidor escuta, por padrão, em `[::1]:50051`.
