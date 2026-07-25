# SX Vault Media — Backlog técnico consolidado

Documento de trabalho para conversão em issues do GitHub. Classificação pela
Matriz de Eisenhower (Urgência × Importância), aplicada ao contexto de
engenharia: **urgente** = risco ativo em produção ou bloqueia o próximo passo
imediato; **importante** = impacto estrutural em correção, escalabilidade ou
manutenibilidade do sistema.

---

## Q1 — Fazer agora (Urgente + Importante)

### `#1` Stream de progresso não encerra após evento terminal
**Labels sugeridas:** `hotfix`, `bug`, `grpc`

Em `create_progress_stream` (stream.rs), o `filter_map` sobre o
`BroadcastStream` continua fazendo poll do receiver mesmo depois de emitir
`Completed`/`Failed`. O gRPC nunca recebe sinal de fim de stream por
iniciativa do servidor. Cada upload concluído deixa um `broadcast::Receiver`
inscrito no bus global pelo resto da vida do processo, junto com a task
Tokio que o segura — vazamento ativo de recurso sob uso real.

**Correção mínima:** `take_while` (ou combinator equivalente) logo após
emitir o item terminal, para que o `Stream` sinalize encerramento ao
runtime gRPC assim que `Completed`/`Failed` for despachado.

**Critério de aceite:**
- Chamada RPC fecha do lado do servidor (trailer gRPC) sem exigir que o
  cliente derrube a conexão manualmente.
- Teste de integração: upload → `is_success` → task e receiver
  correspondentes encerram, sem crescimento de receivers vivos no bus sob
  execução repetida.

---

## Q2 — Agendar (Importante, não urgente — trabalho estrutural)

### `#2` Refatorar event bus: canal `broadcast` global → canal isolado por `media_id`
**Labels:** `debt`, `architecture`, `grpc`

Causa raiz por trás de `#1`. O bus único e global (capacidade 1024,
compartilhado por todos os uploads concorrentes do processo) permite que o
evento terminal de um upload seja descartado por `Lagged` só por volume
gerado por *outros* uploads — não por lentidão do próprio cliente. Combinado
com `Err(_) => None` silencioso em `stream.rs`, gera perda de sinal sem
rastro.

**Proposta:** registry (`DashMap<String, broadcast::Sender<MediaEvent>>` ou
similar) criado sob demanda por `media_id`, dropado ao fim do upload.
Fechar o `Sender` no evento terminal vira o mecanismo natural de
encerramento do stream (`RecvError::Closed` é sinal inequívoco de fim,
diferente de `Lagged`) — resolve `#1` de forma definitiva, não paliativa.

**Dependência:** deve ser planejado junto com `#3` (mesma superfície:
proto + event bus).

---

### `#3` Evoluir `UploadResponse` como relatório final da operação
**Labels:** `debt`, `architecture`, `proto`

Separar as três camadas de feedback hoje misturadas em `ProgressResponse`:
(1) execução da task/orquestração, (2) progresso interno do pipeline
(estágio do operador), (3) progresso de bytes do arquivo em si — hoje
inexistente.

**Decisões de design pendentes:**
- Transporte do `UploadResponse`: `oneof` envelopando `ProgressResponse` +
  `UploadResponse` num único stream, ou dois RPCs separados. A opção `oneof`
  casa melhor com `#1`/`#2`, pois o envio do `UploadResponse` vira o ponto
  natural de fechar o stream.
- Migrar responsabilidade de sucesso/falha de `ProgressResponse.is_success`
  para `UploadResponse.success` — atualizar `stream.rs` e o controller
  juntos.
- Pré-requisito para a camada 3 (progresso de bytes real): não há campo de
  tamanho total esperado do arquivo em nenhum lugar hoje
  (`UploadChunkRequest` nem `MediaProcessCommand`).

---

### `#4` Metadados extraídos (`PipelineContext`) descartados ao fim do pipeline
**Labels:** `debt`, `architecture`, `core`

`reactive_stream_pipe` retorna só `u64` (bytes totais). Todo o
`VideoMetadata`/MIME type extraído por `extract_media_metadata` é perdido
junto com o `PipelineContext`. Bloqueia diretamente `#3` (relatório final
precisará expor esses metadados) e qualquer persistência real futura.

**Proposta:** mudar assinatura para devolver `(u64, PipelineContext)` ou
struct de resultado; propagar até `MediaProcessResult`.

---

### `#5` `original_name` e `expected_content_type` sempre `None` no controller
**Labels:** `debt`, `grpc`

Não bloqueia nada hoje (persistência ainda fictícia), mas vira bloqueador
assim que `#4`/persistência real entrarem — vai exigir extrair esses dados
do primeiro `UploadChunkRequest`, hoje totalmente abstraído para bytes crus
em `tonic_stream_to_byte_stream`.

---

### `#9` Task de processamento desacoplada do lifecycle da conexão do cliente
**Labels:** `debt`, `architecture`, `resiliência`

O `tokio::spawn` no controller gRPC não está amarrado a nenhum
`CancellationToken` ligado à conexão HTTP/2 original. Se o cliente cair no
meio do upload, a task continua rodando até fim do stream ou erro de I/O em
conexão morta — sem forma de abortar processamento de upload abandonado
antecipadamente.

---

### `#10` Ausência de limites de concorrência e tamanho de arquivo
**Labels:** `debt`, `resiliência`, `segurança`

Nenhum semáforo, `tower::limit`, ou verificação de tamanho máximo. O `O(1)`
de memória é verdadeiro *por stream*, mas não há garantia agregada —
volume alto de uploads simultâneos/grandes pode esgotar memória e file
descriptors do processo sem controle algum.

---

### `#11` Validação só roda no fim do stream (fail-at-end, não fail-fast)
**Labels:** `debt`, `design-decision`, `core`

`validate_video_metadata` só executa no `on_complete`. Um upload grande de
conteúdo inválido é lido por inteiro antes de ser rejeitado, mesmo que o
MIME type já tenha sido identificado como inválido nos primeiros 4KB. É uma
escolha de trade-off implícita e não documentada — vale decidir
conscientemente entre rejeitar assim que possível vs. esperar o fim.

---

## Q3 — Delegar / ganho rápido (baixo esforço, resolve incômodo concreto)

### `#6` Semântica de `handle_error` inconsistente entre `reactive_stream_pipe` e `StreamPipe::handle_error`
**Labels:** `debt`, `core`, `documentação-de-contrato`

Não é bug ativo (nenhum operador atual registra `.on_error`), mas é
armadilha para o primeiro que registrar: recuperação em pipe aninhado tenta
todos os operadores até um `Ok`; recuperação no nível top encerra a leitura
inteira do stream e pula todos os flushes, mas reporta sucesso. Barato de
decidir agora (documentação de contrato + talvez teste), caro de descobrir
depois em produção.

---

### `#8` Duplicação de sinalização de sucesso (`Result` + `is_success: bool`)
**Labels:** `debt`, `cleanup`

`MediaProcessResult.is_success` só pode ser `true` no caminho atual. Risco
de código futuro confiar só em `Result::is_ok()` e ignorar o campo se
"sucesso parcial" for introduzido. Ajuste rápido: remover o campo ou
documentar por que os dois sinais coexistem.

---

### `#12` Contagem de receivers do `broadcast::send` descartada
**Labels:** `observability`, `quick-win`

`bus.send(event)` em `event_bus.rs` ignora o retorno (`let _ =`), que inclui
a contagem de receivers ativos no momento do envio — exatamente o dado que
permitiria detectar em produção se `#1`/`#2` de fato resolveram o problema
(zero receivers = evento órfão; contagem crescente = vazamento). Baixo
esforço, alto valor de diagnóstico.

---

## Q4 — Backlog / não bloqueia nada hoje

### `#7` Cópia de bytes a cada operador, mesmo em pass-through
**Labels:** `performance`, `backlog`

`validate_it`/`extract_it` fazem `chunk.to_vec()` mesmo sem modificar
conteúdo. Custo escala com profundidade da pipeline; não compromete o
`O(1)` de memória, mas contradiz a eficiência pretendida sob CPU/alocação à
medida que mais operadores forem adicionados. Migração futura para
`bytes::Bytes` (clone O(1) via refcount).

---

### `#13` Ausência de autenticação/autorização no adaptador gRPC
**Labels:** `security`, `backlog`, `confirmar-suposição`

Pode ser intencional (serviço interno atrás de gateway), mas não está
documentado como suposição explícita em lugar nenhum. Vale registrar e
confirmar antes de qualquer mudança de ambiente de deploy.

---

### `#14` Falta seção viva de "limitações conhecidas" na documentação
**Labels:** `docs`, `backlog`

`arquitetura.md` e `shared_utils_wiki.md` descrevem o sistema em tom de
"pronto e funcionando", sem nenhuma das lacunas mapeadas aqui. Manter uma
seção de limitações conhecidas nos próprios docs (não só no tracker de
issues) evita que a documentação vire um retrato idealizado.

---

### `#15` Falta de observabilidade estruturada (logging/tracing correlacionado)
**Labels:** `observability`, `backlog`

Não há logging estruturado além das strings soltas nos `MediaEvent`. Numa
falha real, a única informação que sobra é a mensagem de erro, sem
contexto de chunk/offset. Não é urgente para o estágio atual, mas custa
caro reconstruir depois que o sistema tiver múltiplos operadores em
produção.

---
