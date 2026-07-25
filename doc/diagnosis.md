# 🏛️ Diretrizes Arquiteturais, Diagnóstico de Runtime & Escopo (Wiki)

> **Contexto:** Documentação formal de auditoria técnica do motor/pipeline ETL assíncrono em Rust.
> **Objetivo:** Estabelecer o diagnóstico de maturidade, definir a matriz de priorização de débitos técnicos e formalizar os Requisitos Não-Funcionais (RNFs) para prontidão de produção (*Production Readiness*).

---

## 1. 🔍 Diagnóstico do Sistema

O projeto reflete um cenário de **senioridade cruzada**:
- **Design de Sistemas (10 anos de bagagem):** Arquitetura hexagonal limpa (`core` / `infra` / `shared`), isolamento por contextos tipados, pipelines baseadas no padrão `FnOperator` e **zero dívida técnica estrutural**.
- **Propriedades de Runtime Rust (4 dias de ecossistema):** Pequenos desvios em nuances de concorrência e gerenciamento de recursos do Tokio (`filter_map` finalizando streams, semântica de lag no `broadcast`, overhead de alocação em `Vec<u8>` e perda de erro raiz por conversão para `String`).

### 📌 Veredito Arquitetural
A espinha dorsal do projeto está **aprovada e saudável**. Nenhum módulo exige refatoração destrutiva ou reescrita. Todos os débitos identificados são cirúrgicos e concentram-se na camada de execução I/O e ciclo de vida de streams.

---

## 2. ⚡ Requisitos Não-Funcionais (RNFs)

Para que o sistema seja considerado pronto para ambiente de produção, ele deve satisfazer as seguintes restrições:

| ID | Categoria | Descrição do Requisito Não-Funcional | Critério de Aceite / Métrica |
| :--- | :--- | :--- | :--- |
| **RNF-01** | **Correctness (Streams)** | Fluxos reativos não podem ser encerrados por falhas de parsing ou retornos nulos operacionais. | NENHUM `Stream` pode fechar de forma prematura devido ao uso de combinators como `filter_map`. |
| **RNF-02** | **Backpressure & Sync** | Canais de mensageria assíncrona devem tolerar picos de tráfego sem queda indevida de dados. | Canais `broadcast` devem possuir capacidade delimitada (*bounded*) e tratamento explícito de `RecvError::Lagged`. |
| **RNF-03** | **Zero-Copy Efficiency** | A travessia de dados no pipeline ETL deve minimizar alocações de memória na Heap. | Uso obrigatório de `bytes::Bytes` nas fronteiras de I/O em substituição a `Vec<u8>`. |
| **RNF-04** | **Observabilidade de Erros** | O sistema não pode perder a árvore/causa raiz de erros ao propagar falhas entre camadas. | Proibido o uso de `.map_err(|e| e.to_string())`. Todos os erros devem usar Enums tipados via `thiserror`. |
| **RNF-05** | **Integridade de Persistência** | As operações da camada de infraestrutura devem interagir diretamente com o storage real. | Eliminação completa de retornos mockados ou caminhos de arquivos fictícios no ciclo de gravação. |

---

## 3. 🎯 Matriz de Priorização e Escopo

> ⚠️ **REFEITURA DE ENGENHARIA:** Fica **congelado** qualquer refinamento ou adição de novas abstrações em `shared/utils` até que os itens de prioridade P0 e P1 estejam 100% resolvidos com testes de integração.
