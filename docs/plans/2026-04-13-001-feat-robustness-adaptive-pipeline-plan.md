---
title: "feat: Robustez e auto-adaptação do pipeline peopledd"
type: feat
status: active
date: 2026-04-13
---

# feat: Robustez e auto-adaptação do pipeline peopledd

## Overview

O pipeline já tem resiliência básica: retries limitados, circuit breakers por falha consecutiva e
degradação via `ServiceLevel`. O salto necessário é passar de **"resiliente com retries fixos"**
para **"diagnóstico causal + roteamento adaptativo por causa + memória de domínio"**.

O plano tem três fases sequenciais e cada fase entrega valor isoladamente:

- **Fase 1 — Hardening da coleta:** tipar falhas, multi-superfície RI, assessment causal.
- **Fase 2 — Adaptive planner:** RecoveryPlanner, circuit breakers ponderados, source memory.
- **Fase 3 — Fusão e calibração:** score multi-sinal, identidade progressiva de pessoas, pipeline offline.

## Problem Frame

Hoje o pipeline não distingue "RI retornou HTML vazio" de "RI deu timeout" de "LLM de extração
falhou". Consequência: a resposta é sempre a mesma — retornar `GovernanceSnapshot()` vazio — e a
política adaptativa não tem base para escolher a próxima ação diferente.

Também não há memória entre runs: o pipeline recomeça do zero sem saber que a URL de governança
de uma empresa está em `/ri/governanca/`, que o site usa JS pesado, ou que certos aliases de busca
funcionam melhor.

## Requirements Trace

- R1. Falhas de fonte retornam tipo estruturado com `failure_mode`, não só silêncio.
- R2. O nó n1 tenta múltiplas superfícies do site RI antes de cair no fallback de web privada.
- R3. A política adaptativa seleciona a próxima ação com base em `failure_mode`, não só score baixo.
- R4. CircuitBreakers pesam tipo e frequência de falha, não só contagem consecutiva.
- R5. Há persistência de dados sobre fontes por empresa/domínio reusada entre runs.
- R6. A fusão de governança pondera temporalidade e suporte cruzado, não só rank fixo de fonte.
- R7. Resolução de pessoas usa features progressivas e negativas para reduzir ambiguidade.
- R8. Um pipeline offline coleta outcomes por ação para calibrar thresholds adaptativos.

## Scope Boundaries

- Não altera a API pública do `FinalReport` (campos adicionais são `Optional`).
- Não altera contratos Pydantic existentes — apenas estende com novos campos opcionais.
- Não adiciona dependência de banco de dados — `source_memory` usa JSON por diretório de empresa.
- Não implementa RL — o calibrator usa análise estatística simples sobre JSONs de telemetria.
- Fase 3 (calibração offline) não bloqueia as Fases 1 e 2.
- `use_apify` e `use_browserless` continuam como flags reservadas; não são wired neste plano.

## Context & Research

### Padrões relevantes a seguir

- `src/peopledd/runtime/adaptive_models.py` — `AssessmentGap`, `AdaptiveActionKind`, `PhaseAssessment`; estender com novos literais sem quebrar os existentes.
- `src/peopledd/runtime/circuit_breaker.py` — `SourceCircuitBreaker`; substituição drop-in pela versão ponderada.
- `src/peopledd/runtime/adaptive_policy.py` — `DefaultAdaptivePolicy`; manter interface `decide_*` com retorno `(AdaptiveActionKind, str, str|None)`.
- `src/peopledd/runtime/phase_assessment.py` — funções puras `assess_after_*`; tipar outputs com novos gap kinds.
- `src/peopledd/services/ri_scraper.py` — já tem intent crawler e suffix fallback; estender para `RI SurfaceDiscoverer`.
- `src/peopledd/services/governance_fusion_judge.py` — `rule_based_fusion`, `llm_judge_fusion`, `_track_rank`; estender com score composto.
- `src/peopledd/nodes/n2_person_resolution.py` — lógica de confiança progressiva; introduzir `PersonIdentityCandidate`.
- `tests/test_adaptive_policy.py`, `tests/test_circuit_breaker.py` — estilo de mock e asserção a seguir.
- `tests/test_semantic_fusion.py` — estilo de teste para fusão.

### Lacunas confirmadas na pesquisa

- `governance_fusion_judge.build_resolved_snapshot` não repõe committees da fusão no snapshot resolvido.
- n2 usa `governance_reconciliation` (n1b), não o `resolved_snapshot` da fusão n1c — por design declarado.
- `_ingest_current` captura toda `Exception` e retorna `GovernanceSnapshot()` sem distinção de causa.
- Nenhum mecanismo de memória entre runs.
- Circuit breakers só contam falhas consecutivas — sem janelas, sem peso por tipo.

## Key Technical Decisions

- **`SourceAttemptResult` é a primitiva de falha:** cada chamada de fonte retorna esse objeto (com `failure_mode` tipado), nunca silencia e levanta. Isso deixa o assessment causal no lugar certo.
- **`RecoveryPlanner` substitui a tabela if/else de `DefaultAdaptivePolicy`:** mantém a mesma interface externa, mas internamente consulta um catálogo declarativo de ações com `preconditions`, `cost`, `expected_gain`.
- **`WeightedCircuitBreaker` é drop-in para `SourceCircuitBreaker`:** mantém `allow()`, `record_success()`, `record_failure(weight)`, `snapshot()`; `default_breaker_set()` retorna instâncias novas.
- **Source memory é JSON por domínio, leitura/escrita atômica:** não requer dependência nova; acoplada ao `RunContext` via referência opcional ao `SourceMemoryStore`.
- **Fusão multi-sinal é retrocompatível:** `rule_based_fusion` passa a usar `compute_fusion_score(obs)` em vez de `_track_rank`; sem mudança de assinatura.
- **Calibração offline é ferramenta CLI separada:** lê telemetria JSON dos runs, nunca altera o pipeline online.

## Open Questions

### Resolved During Planning

- **Onde guardar `source_memory`?** No diretório base de output, subpasta `_source_memory/<domain_hash>.json`. Não polui runs individuais.
- **`SourceAttemptResult` deve ser Pydantic ou dataclass?** Dataclass simples (não serializada para artefatos externos, só usada intra-run).
- **Qual granularidade do `failure_mode`?** Literal com ~10 valores: `timeout`, `anti_bot`, `low_content`, `pdf_only`, `llm_extract_failed`, `schema_mismatch`, `network_error`, `not_found`, `parse_error`, `budget_exhausted`. Extensível sem quebrar.
- **`PersonIdentityCandidate` onde vive?** Em `src/peopledd/runtime/identity_models.py` (novo), para não poluir `contracts.py` com lógica interna.

### Deferred to Implementation

- Thresholds exatos do `WeightedCircuitBreaker` por fonte (a ser calibrado na Fase 3).
- Heurística de extração de mandato atual vs histórico para score temporal (depende de ver dados reais).
- Estrutura exata do `RecoveryPlanner` catalog YAML/Python dict (depende de quantas ações existirão ao final da Fase 1).
- Quais métricas o calibrador offline vai expor além de `avg_sl` e `recovery_hit_rate`.

## High-Level Technical Design

> *Ilustração da abordagem — guia direcional para revisão, não especificação de implementação.*

### Fluxo de chamada com SourceAttemptResult

```
n1._ingest_current(ri_url) →
  RIScraper.scrape_board_multi_surface(ri_url) →
    surface_discoverer.discover(ri_url) → [surfaces]
    for surface in surfaces:
      attempt = fetch_surface(surface)   # retorna SourceAttemptResult
      if attempt.success: break
      breaker.record_failure(weight=weight_for(attempt.failure_mode))
  → SourceAttemptResult(success, failure_mode, content, freshness_detected, ...)
n1 constrói GovernanceSnapshot + registra attempt no ctx
```

### RecoveryPlanner (catálogo de ações)

```
RecoveryPlanner.next_action(phase, assessment, ctx, breakers) →
  candidates = [a for a in catalog if a.phase == phase
                and all(pre(assessment, ctx, breakers) for pre in a.preconditions)]
  ranked = sort by (expected_gain - a.cost) desc
  return ranked[0] if candidates else ("degrade_and_continue", ...)
```

### Source Memory read/write

```
SourceMemoryStore.load(company_key) → CompanyMemory
  # read JSON; merge fields on write
CompanyMemory:
  useful_ri_surfaces: list[str]
  failed_ri_strategies: list[str]
  company_aliases: list[str]
  person_observations: list[PersonMemoryEntry]
  last_updated: str
SourceMemoryStore.save(company_key, memory)
```

### Multi-signal fusion score

```
compute_fusion_score(obs: GovernanceObservation) → float:
  authority = _track_rank(obs.source_track) / 4.0        # [0..1]
  freshness = _freshness_factor(obs.as_of_date)           # [0..1]
  text_quality = _snippet_score(obs.evidence_span)        # [0..1]
  return 0.5*authority + 0.3*freshness + 0.2*text_quality
```

## Implementation Units

---

### FASE 1 — Hardening da coleta

---

- [ ] **Unit 1: `SourceAttemptResult` — primitiva de falha estruturada**

**Goal:** Toda chamada de fonte retorna `SourceAttemptResult` com `failure_mode` tipado em vez de capturar exceção silenciosamente.

**Requirements:** R1

**Dependencies:** Nenhuma — é o fundamento das demais unidades.

**Files:**
- Create: `src/peopledd/runtime/source_attempt.py`
- Modify: `src/peopledd/services/ri_scraper.py` (retornar `SourceAttemptResult` em `scrape_board`)
- Modify: `src/peopledd/nodes/n1_governance_ingestion.py` (`_ingest_current` consome `SourceAttemptResult`)
- Test: `tests/test_source_attempt.py`

**Approach:**
- Dataclass `SourceAttemptResult` com campos: `success: bool`, `failure_mode: SourceFailureMode | None`, `content: str | None`, `content_words: int`, `strategy_used: str | None`, `latency_ms: float`, `freshness_detected: str | None`, `schema_confidence: float`.
- `SourceFailureMode` é `Literal["timeout", "anti_bot", "low_content", "pdf_only", "llm_extract_failed", "schema_mismatch", "network_error", "not_found", "parse_error", "budget_exhausted"]`.
- `RIScraper.scrape_board` passa a retornar `SourceAttemptResult` além de `GovernanceSnapshot`; `_ingest_current` registra o resultado no `ctx.log` com `failure_mode` no payload.
- `n1_governance_ingestion._ingest_current` não chama mais `try/except Exception` silencioso; delega ao scraper que encapsula falhas dentro de `SourceAttemptResult`.

**Patterns to follow:**
- `src/peopledd/runtime/adaptive_models.py` — estilo dataclass simples, sem Pydantic pesado.
- `src/peopledd/runtime/circuit_breaker.py` — snapshot dict serializável.

**Test scenarios:**
- Happy path: `scrape_board` retorna `SourceAttemptResult(success=True, failure_mode=None, content="...", content_words=200)`.
- Edge case: scraper retorna menos de 50 palavras → `failure_mode="low_content"`, `success=False`.
- Error path: httpx lança `TimeoutException` → `failure_mode="timeout"`, `success=False`.
- Error path: LLM de extração lança `openai.APIError` → `failure_mode="llm_extract_failed"`.
- Error path: nenhuma `OPENAI_API_KEY`, `budget_exhausted` → `failure_mode="budget_exhausted"`.
- Integration: `_ingest_current` com resultado `success=False` registra `ctx.log("gap", "n1", ...)` com `failure_mode` no payload e retorna `GovernanceSnapshot()`.

**Verification:**
- Todos os caminhos de `scrape_board` retornam `SourceAttemptResult`, não levantam para o chamador.
- `failure_mode` nunca é `None` quando `success=False`.
- `ctx.trace` contém o `failure_mode` após `_ingest_current` em run offline.

---

- [ ] **Unit 2: `RISurfaceDiscoverer` — aquisição multi-superfície do RI**

**Goal:** Antes de cair em private web discovery, o n1 tenta sistematicamente múltiplas superfícies do site RI (subpáginas de governança, links do menu, PDFs, fallback de sufixos).

**Requirements:** R2

**Dependencies:** Unit 1 (usa `SourceAttemptResult`).

**Files:**
- Create: `src/peopledd/services/ri_surface_discoverer.py`
- Modify: `src/peopledd/services/ri_scraper.py` (método `scrape_board_multi_surface`)
- Modify: `src/peopledd/nodes/n1_governance_ingestion.py` (`_ingest_current` usa o novo método)
- Test: `tests/test_ri_surface_discoverer.py`

**Approach:**
- `RISurfaceDiscoverer.discover(base_url, html_content) → list[str]`: varre `<a>` tags com regex de palavras-chave de governança (já existe parcialmente no intent crawler atual), retorna lista de URLs candidatas rankadas por relevância de keyword.
- `RIScraper.scrape_board_multi_surface(ri_url) → tuple[GovernanceSnapshot, list[SourceAttemptResult]]`:
  1. Faz fetch da URL base → obtém HTML.
  2. `RISurfaceDiscoverer.discover(base_url, html)` → candidatas.
  3. Para cada candidata (limite configurável, ex.: 5), faz `_scrape_url` + extração.
  4. Para quando encontrar um snapshot com pessoas.
  5. Se nenhuma: tenta sufixos estáticos (já existente).
  6. Retorna melhor snapshot + lista de todos os attempts.
- `_ingest_current` usa `scrape_board_multi_surface`; registra os attempts via `ctx.log`.
- Fonte de ranking de superfícies: keywords, profundidade de URL, presença de tabelas no HTML.

**Patterns to follow:**
- Intent crawler e suffix fallback já em `src/peopledd/services/ri_scraper.py` — consolidar e estender, não duplicar.
- `MultiStrategyScraper` — padrão de parada em primeiro resultado adequado.

**Test scenarios:**
- Happy path: HTML base contém link `/ri/conselho-administracao/`; discoverer retorna essa URL em posição 1; scrape da subpágina retorna snapshot com 3 board members.
- Edge case: HTML base sem links de governança → discoverer retorna lista vazia → fallback de sufixos.
- Edge case: primeira subpágina retorna `low_content` → tenta próxima; segundo retorna snapshot válido.
- Error path: todas as superfícies falham → retorna `GovernanceSnapshot()` + lista de attempts todos com `success=False`.
- Integration: `_ingest_current` com site que só tem PDF → attempts registram `failure_mode="pdf_only"` para cada superfície.

**Verification:**
- Com site de teste que tem links de governança, `scrape_board_multi_surface` encontra a subpágina correta.
- `list[SourceAttemptResult]` retornado tem comprimento >= 1 em todos os casos.
- Sem regressão em testes existentes de `n1_governance_ingestion`.

---

- [ ] **Unit 3: Assessment causal por fase — gaps tipados por `failure_mode`**

**Goal:** As funções `assess_after_*` em `phase_assessment.py` produzem gaps que indicam a causa da deficiência, não só o score abaixo do threshold.

**Requirements:** R3

**Dependencies:** Unit 1 (gaps podem referenciar `failure_mode` coletado no `RunContext`).

**Files:**
- Modify: `src/peopledd/runtime/adaptive_models.py` (expandir `AssessmentGapKind`)
- Modify: `src/peopledd/runtime/phase_assessment.py` (aceitar contexto de attempts no assessment n1)
- Modify: `src/peopledd/runtime/adaptive_policy.py` (decisões consultam `failure_mode` dos gaps)
- Test: `tests/test_phase_assessment.py` (novo ou estender `test_adaptive_policy.py`)

**Approach:**
- Expandir `AssessmentGapKind` com literais: `"ri_scrape_failed"`, `"ri_low_content"`, `"ri_anti_bot"`, `"llm_budget_exhausted"`, `"formal_cnpj_missing"`, `"formal_parser_partial"`.
- `assess_after_n1_ingestion` recebe `ri_failure_mode: SourceFailureMode | None = None`; quando presente, emite gap específico em vez de genérico `"current_governance_weak"`.
- `DefaultAdaptivePolicy.decide_n1_*` pode consultar o gap kind para escolher ação diferente:
  - `ri_anti_bot` → tentar Browserless antes de FRE retry.
  - `ri_low_content` → tentar `RISurfaceDiscoverer` (já presente na Unit 2).
  - `llm_budget_exhausted` → `degrade_and_continue` imediato (não adianta retry).
- O contexto de `failure_mode` dos attempts é passado do `GraphRunner` para o assessment via parâmetro; não vira estado global.

**Patterns to follow:**
- `src/peopledd/runtime/adaptive_models.py` — padrão de extensão de Literal sem quebrar existente.
- `src/peopledd/runtime/adaptive_policy.py` — retorno `(AdaptiveActionKind, rationale, recovery_key)`.

**Test scenarios:**
- Happy path: `ri_failure_mode=None` → gap `"current_governance_weak"` (comportamento atual preservado).
- Edge case: `ri_failure_mode="anti_bot"` → gap `"ri_anti_bot"` e policy retorna ação diferente de `ri_failure_mode="low_content"`.
- Error path: `ri_failure_mode="llm_budget_exhausted"` → policy retorna `"degrade_and_continue"` sem attempt de recovery.
- Integration: após run offline com site mockado com anti-bot, `adaptive_decisions` no telemetry registra gap `"ri_anti_bot"` e ação `"degrade_and_continue"`.

**Verification:**
- `PhaseAssessment.gaps` nunca mistura `"current_governance_weak"` e `"ri_anti_bot"` para o mesmo problema.
- Testes existentes em `test_adaptive_policy.py` continuam passando.

---

### FASE 2 — Adaptive planner e source memory

---

- [ ] **Unit 4: `RecoveryPlanner` — catálogo declarativo de ações**

**Goal:** Substituir a tabela `if/else` de `DefaultAdaptivePolicy` por um planner que consulta um catálogo de ações com `preconditions`, `cost`, `expected_gain` — mantendo a mesma interface externa.

**Requirements:** R3

**Dependencies:** Unit 3 (gaps causais são inputs do planner).

**Files:**
- Create: `src/peopledd/runtime/recovery_planner.py`
- Modify: `src/peopledd/runtime/adaptive_policy.py` (delegar `decide_*` ao planner)
- Test: `tests/test_recovery_planner.py`

**Approach:**
- `RecoveryAction` dataclass: `kind: AdaptiveActionKind`, `phase`, `cost: float`, `expected_gain: float`, `preconditions: list[Callable[[PhaseAssessment, RunContext, breakers], bool]]`, `recovery_key: str | None`.
- `RecoveryPlanner.next_action(phase, assessment, ctx, breakers) -> (AdaptiveActionKind, str, str|None)`:
  - Filtra ações do catálogo por fase e precondições satisfeitas.
  - Ordena por `(expected_gain - cost)` decrescente.
  - Retorna a top-1 ou `("degrade_and_continue", ...)` se catálogo vazio.
- Catálogo inicial espelha exatamente o comportamento atual dos `decide_*` em `DefaultAdaptivePolicy`; diferença: agora é editável sem mudar fluxo.
- `DefaultAdaptivePolicy.decide_*` delegam ao `RecoveryPlanner`; interface e retorno permanecem iguais.
- Registro da decisão continua em `ctx.record_adaptive_decision`.

**Patterns to follow:**
- `src/peopledd/runtime/adaptive_policy.py` — retorno `(kind, rationale, key)`.
- `src/peopledd/runtime/adaptive_models.py` — `AdaptiveDecisionRecord`.

**Test scenarios:**
- Happy path: catálogo com duas ações candidatas para mesma fase; planner escolhe a de maior `(expected_gain - cost)`.
- Edge case: todas as precondições bloqueadas → retorna `("degrade_and_continue", ...)`.
- Edge case: recovery budget esgotado → `ctx.recovery_allowed(key)=False` bloqueia ação mais cara; planner escolhe próxima.
- Error path: catálogo vazio para uma fase → `"degrade_and_continue"` sem exceção.
- Integration: `DefaultAdaptivePolicy.decide_n1_fre_extended` produz mesmo resultado de antes após refactor para usar planner.

**Verification:**
- `test_adaptive_policy.py` existente passa sem modificação nos casos de comportamento preservado.
- Planner não lança exceção com catálogo vazio.
- `AdaptiveDecisionRecord` no trace é idêntico ao produzido antes do refactor.

---

- [ ] **Unit 5: `WeightedCircuitBreaker` — ponderação por tipo de falha**

**Goal:** Substituir contagem consecutiva simples por janela deslizante com peso por tipo de falha, health score por fonte e roteamento de fallback quando breaker está degradado.

**Requirements:** R4

**Dependencies:** Unit 1 (`SourceAttemptResult` provê `failure_mode` para peso).

**Files:**
- Modify: `src/peopledd/runtime/circuit_breaker.py` (substituir `SourceCircuitBreaker` por `WeightedCircuitBreaker`, manter `default_breaker_set()`)
- Modify: `src/peopledd/runtime/graph_runner.py` (`_breaker_failure(key, weight=...)`)
- Test: `tests/test_circuit_breaker.py` (estender)

**Approach:**
- `WeightedCircuitBreaker` mantém a mesma API pública (`allow()`, `record_success()`, `record_failure(weight=1.0)`, `snapshot()`).
- Internamente: lista de `(timestamp, weight)` em janela de `window_sec` (ex.: 120s); `health_score = 1.0 - sum(weights_in_window) / threshold_weight`.
- `allow()` retorna `True` quando `health_score > 0.0` (closed ou half-open), `False` quando `health_score <= 0.0` (open).
- Pesos por `failure_mode`:
  - `timeout` → 1.5
  - `anti_bot` → 2.0
  - `network_error` → 1.0
  - `low_content` → 0.3
  - `parse_error` → 0.5
  - outros → 1.0
- `GraphRunner._breaker_failure(key, weight=1.0)` repassa weight calculado por `failure_mode`.
- `snapshot()` inclui `health_score` para telemetria.

**Patterns to follow:**
- `src/peopledd/runtime/circuit_breaker.py` — interface pública existente; testes em `tests/test_circuit_breaker.py`.
- `monotonic()` já usado para timing.

**Test scenarios:**
- Happy path: várias `record_failure(weight=0.3)` não abrem breaker; `record_failure(weight=2.0)` combinadas abrem.
- Edge case: janela expira → falhas antigas saem do histórico → `allow()` volta `True`.
- Edge case: `record_success()` reseta histórico inteiro.
- Error path: `threshold_weight <= 0` no construtor → raise `ValueError`.
- Integration: `_breaker_failure("ri", weight=2.0)` seguido de `_breaker_failure("ri", weight=2.0)` → `breakers["ri"].allow()` retorna `False` antes de atingir threshold antigo de 4 consecutivas.

**Verification:**
- Testes de `test_circuit_breaker.py` existentes adaptados para a nova semântica de weight passam.
- `snapshot()` inclui `health_score` entre 0.0 e 1.0.
- `default_breaker_set()` retorna `WeightedCircuitBreaker` em todos os slots.

---

- [ ] **Unit 6: `SourceMemoryStore` — memória por empresa/domínio**

**Goal:** Persistir entre runs dados sobre superfícies úteis, aliases e estratégias que falharam para uma empresa, usando JSON por chave de domínio.

**Requirements:** R5

**Dependencies:** Unit 2 (grava superfícies úteis), Unit 1 (grava failure modes por domínio).

**Files:**
- Create: `src/peopledd/runtime/source_memory.py`
- Modify: `src/peopledd/runtime/context.py` (campo opcional `source_memory: SourceMemoryStore | None`)
- Modify: `src/peopledd/runtime/graph_runner.py` (inicializa e usa `SourceMemoryStore` quando `output_dir` configurado)
- Modify: `src/peopledd/nodes/n1_governance_ingestion.py` (consulta e atualiza memória de RI surfaces)
- Test: `tests/test_source_memory.py`

**Approach:**
- `CompanyMemory` dataclass: `company_key: str`, `useful_ri_surfaces: list[str]`, `failed_ri_strategies: list[str]`, `company_aliases: list[str]`, `person_observations: list[dict]`, `last_updated: str`.
- `SourceMemoryStore(base_dir: Path)`:
  - `load(company_key) -> CompanyMemory | None` — lê `base_dir/_source_memory/{hash}.json`.
  - `save(company_key, memory)` — escrita atômica via `tmp` + `rename`.
  - `update_ri_success(company_key, surface_url)` — move URL para topo de `useful_ri_surfaces`.
  - `update_ri_failure(company_key, strategy)` — appenda a `failed_ri_strategies`.
- `company_key` = `sha256(normalized_company_name + "::" + country)[:12]`.
- `RISurfaceDiscoverer.discover` consulta memória: prioriza `useful_ri_surfaces` se disponível.
- `RunContext` recebe referência ao store; n1 obtém store via `RunContext`.

**Patterns to follow:**
- `src/peopledd/runtime/pipeline_state.py` — escrita atômica via JSON + `Path.write_text`.
- `src/peopledd/utils/io.py` — `write_json`.

**Test scenarios:**
- Happy path: primeira run grava `useful_ri_surfaces=["/ri/governanca/"]`; segunda run lê e prioriza essa URL.
- Edge case: arquivo inexistente → `load()` retorna `None`; sem erro.
- Edge case: JSON corrompido → `load()` retorna `None`, loga warning, não levanta.
- Error path: escrita falha por `PermissionError` → captura, loga, continua run sem crash.
- Integration: pipeline completo com `tmp_path`, empresa X; após run bem-sucedida, `_source_memory/` contém JSON com superfície RI usada.

**Verification:**
- Segunda run de mesma empresa começa com `useful_ri_surfaces` já preenchido.
- `tmp_path/_source_memory/` existe após run completa.
- Sem regressão em `test_pipeline.py`.

---

### FASE 3 — Fusão e calibração

---

- [ ] **Unit 7: Score multi-sinal na fusão de governança**

**Goal:** Substituir `_track_rank` fixo na fusão por `compute_fusion_score` que pondera autoridade de fonte, frescor temporal e qualidade textual do snippet.

**Requirements:** R6

**Dependencies:** Nenhuma das Fases 1–2; pode ser implementada em paralelo após Unit 3.

**Files:**
- Modify: `src/peopledd/services/governance_fusion_judge.py` (`rule_based_fusion`, nova função `compute_fusion_score`)
- Modify: `src/peopledd/services/governance_observation_builder.py` (adicionar `as_of_date` e `snippet_length` às observações quando disponíveis)
- Modify: `src/peopledd/models/contracts.py` (`GovernanceObservation` + campo `as_of_date: str | None`)
- Test: `tests/test_semantic_fusion.py` (estender)

**Approach:**
- `compute_fusion_score(obs: GovernanceObservation) -> float`:
  - `authority = _track_rank(obs.source_track) / 4.0`
  - `freshness = _freshness_factor(obs.as_of_date)` onde `_freshness_factor` decai de 1.0 a 0.0 linearmente de 0 a 730 dias; `None` → 0.5 (incerto).
  - `text_quality = min(1.0, len(obs.evidence_span.snippet) / 200)` se snippet existir, senão 0.3.
  - `return 0.5 * authority + 0.3 * freshness + 0.2 * text_quality`
- `rule_based_fusion` usa `compute_fusion_score` para ordenar em vez de `(_track_rank, source_confidence)`.
- `GovernanceObservation` ganha campo `as_of_date: str | None = None`.
- `governance_observation_builder` preenche `as_of_date` a partir de `snapshot.as_of_date`.
- Também corrigir o gap apontado na pesquisa: `build_resolved_snapshot` deve materializar committees da fusão no snapshot.

**Patterns to follow:**
- `src/peopledd/services/governance_fusion_judge.py` — `_track_rank`, `rule_based_fusion`.
- `src/peopledd/runtime/staleness.py` — `_freshness_score` já existente para referência de decaimento.

**Test scenarios:**
- Happy path: observação `formal_fre` com data recente e snippet longo → maior score que `current_ri` com data antiga e snippet curto.
- Edge case: `as_of_date=None` → `freshness=0.5`, não erro.
- Edge case: snippet vazio → `text_quality=0.3`, não divide por zero.
- Edge case: duas observações com score idêntico → desempate por `source_confidence` original.
- Integration: após fusão com dados de dois tracks, `GovernanceFusionDecision.confidence` reflete score multi-sinal, não mais só `0.5 + 0.08 * support_count`.
- Integration: committees presentes na fusão aparecem no `resolved_snapshot.committees` (bug fix).

**Verification:**
- `test_semantic_fusion.py` valida que observação mais recente é preferida sobre mais antiga de mesma fonte.
- `resolved_snapshot.committees` não é mais lista vazia quando fusão tem decisões de committee.

---

- [ ] **Unit 8: Resolução progressiva de pessoas com features negativas**

**Goal:** Introduzir `PersonIdentityCandidate` com resolução em múltiplos passos (nome → nome+empresa → nome+empresa+cargo) e features negativas (empresa incompatível, cargo divergente) para reduzir ambiguidade.

**Requirements:** R7

**Dependencies:** Nenhuma das Fases 1–2.

**Files:**
- Create: `src/peopledd/runtime/identity_models.py`
- Modify: `src/peopledd/nodes/n2_person_resolution.py` (usar `PersonIdentityCandidate` na resolução)
- Test: `tests/test_identity_resolution.py`

**Approach:**
- `PersonIdentityCandidate` dataclass: `observed_name: str`, `target_company: str`, `expected_organ: str`, `positive_signals: list[str]`, `negative_signals: list[str]`, `candidates: list[HarvestCandidate]`, `resolution_round: int`.
- Resolução em 3 rounds:
  1. `match_by_name(candidates, target_company)` → filtra por nome.
  2. Se ambíguo (>= 2 com score próximo): `match_by_company(candidates, target_company)` — verifica `company_match`.
  3. Se ainda ambíguo: `match_by_organ(candidates, expected_organ)` — verifica cargo esperado vs título retornado.
- Features negativas explícitas:
  - Candidato com empresa completamente diferente → `negative_signal = "company_mismatch"`, peso -0.3.
  - Candidato com cargo incompatível (ex.: espera conselho, encontra CEO de empresa errada) → peso -0.2.
  - Ambiguidade de LinkedIn (mesmo nome, dois perfis com empresa diferente) → penalidade de confiança.
- `PersonResolution.negative_signals: list[str] = []` — novo campo opcional em `contracts.py`.

**Patterns to follow:**
- `src/peopledd/nodes/n2_person_resolution.py` — lógica de confiança e ambiguidade atual.
- `src/peopledd/models/contracts.py` — `PersonResolution`.

**Test scenarios:**
- Happy path: único candidato com nome + empresa coincidindo → `resolution_status=RESOLVED`, `resolution_round=1`.
- Edge case: dois candidatos com nome parecido, empresas diferentes → round 2 resolve por empresa.
- Edge case: zero candidatos no Harvest → `NOT_FOUND`, sem attempt de rounds 2/3.
- Error path: round 3 ainda ambíguo → `AMBIGUOUS` com `negative_signals` registrados.
- Integration: `PersonResolution.negative_signals` não vazio aparece no `people_resolution.json` após run completa com ambiguidade mockada.

**Verification:**
- Testes existentes em `test_n2_n3_harvest.py` passam.
- `resolution_round` é 1 para casos claros e > 1 para casos que precisaram de escalação.

---

- [ ] **Unit 9: Calibration pipeline offline**

**Goal:** CLI que lê telemetria JSON de múltiplas corridas, agrupa por tipo de gap e ação adaptativa, e reporta quais combinações produziram maior ganho de cobertura — para calibrar thresholds.

**Requirements:** R8

**Dependencies:** Fases 1 e 2 (telemetria mais rica), mas pode ser implementada sobre telemetria atual.

**Files:**
- Create: `src/peopledd/tools/calibrate.py` (CLI standalone)
- Create: `tests/test_calibrate.py`

**Approach:**
- `calibrate.py --runs-dir <dir>` percorre `<dir>/*/run_summary.json` e `<dir>/*/run_trace.json`.
- Extrai por run: `service_level`, `adaptive_decisions`, `gaps`, `recovery_counts`.
- Agrupa por `(gap_kind, action_taken)` → calcula `avg_sl_after_action`, `recovery_hit_rate` (ação levou a SL melhor que degrade?).
- Gera `calibration_report.json` com:
  - threshold atual por gap vs threshold sugerido baseado em percentil da distribuição de scores reais.
  - top-3 ações por gap_kind ordenadas por hit_rate.
- Também emite `calibration_report.md` human-readable.
- Não modifica o pipeline — é read-only sobre artefatos existentes.

**Patterns to follow:**
- `src/peopledd/runtime/run_inspect.py` — `list_runs`, `read_run_summary`.
- `src/peopledd/cli.py` — padrão de argparse.

**Test scenarios:**
- Happy path: 10 runs sintéticos com JSON fixos → relatório com hit_rate correto para `("ri_low_content", "retry_ri_multi_surface")`.
- Edge case: diretório sem runs válidos → relatório vazio, exit code 0.
- Edge case: `run_summary.json` corrompido → run ignorada com warning, restante processado.
- Error path: `--runs-dir` inexistente → stderr + exit code 2.

**Verification:**
- `calibration_report.json` gerado com keys `gap_kind`, `action`, `avg_sl`, `hit_rate` para cada combinação.
- `calibration_report.md` legível por humano com recomendações de threshold.

---

## System-Wide Impact

- **Interaction graph:** `GraphRunner._run_governance_phase` passa a receber/registrar `SourceAttemptResult`; `_breaker_failure` recebe `weight`; `DefaultAdaptivePolicy` delega ao `RecoveryPlanner`; `n1_governance_ingestion` consulta `SourceMemoryStore` via `RunContext`.
- **Error propagation:** Falhas de fonte nunca levantam fora do módulo de origem — são encapsuladas em `SourceAttemptResult`. O pipeline continua. Erros de I/O na `SourceMemoryStore` são capturados e logados, não propagados.
- **State lifecycle risks:** `SourceMemoryStore` usa escrita atômica (`tmp + rename`). Leitura nunca bloqueia o pipeline — retorna `None` se arquivo ausente/corrompido. Checkpoint fingerprint (já implementado) cobre rollback de estado.
- **API surface parity:** `FinalReport` não muda — novos campos em `PersonResolution.negative_signals` são `Optional` com default `[]`. `GovernanceObservation.as_of_date` é `Optional`.
- **Integration coverage:** Testes de `test_pipeline.py` devem cobrir uma corrida completa com `SourceMemoryStore` em `tmp_path` após Fase 2.
- **Unchanged invariants:** Interface CLI (`peopledd --input-json`), formato de `run_summary.json`, `FinalReport` schema (adições retrocompatíveis), `read_checkpoint` / `write_checkpoint`.

## Phased Delivery

### Fase 1 — Hardening da coleta (Units 1–3)
Entregável: `SourceAttemptResult` fluindo no pipeline, multi-surface RI, assessment com gaps causais. Testes passam. Sem mudança de comportamento observável externamente — apenas mais telemetria.

### Fase 2 — Adaptive planner e memória (Units 4–6)
Entregável: `RecoveryPlanner`, `WeightedCircuitBreaker`, `SourceMemoryStore`. Runs subsequentes de mesma empresa ficam mais rápidas (memória de superfícies). Telemetria mais rica em `adaptive_decisions`.

### Fase 3 — Fusão e calibração (Units 7–9)
Entregável: fusão com score temporal, resolução progressiva de pessoas, ferramenta de calibração. Pode rodar calibrador sobre runs da Fase 1+2 para sugerir ajustes de threshold.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `SourceAttemptResult` não cobre todos os caminhos de falha do `MultiStrategyScraper` | Revisar todos os `except` no `ri_scraper.py` e `scraper.py` ao implementar Unit 1 |
| `RecoveryPlanner` com catálogo errado muda comportamento silenciosamente | Unit 4 exige test de comportamento preservado para cada caso de `DefaultAdaptivePolicy` atual |
| `SourceMemoryStore` JSON corrompido entre runs derruba pipeline | Unit 6 obriga `try/except` em `load()` com fallback `None` |
| Score multi-sinal (Unit 7) pode piorar fusão em casos cobertos pelo score atual simples | Manter `_track_rank` como tiebreaker; rodar gold set antes e depois para comparação |
| Calibrador (Unit 9) com amostras pequenas gera recomendações ruidosas | Documentar mínimo de runs sugerido no relatório; não aplicar sugestões automaticamente |

## Documentation / Operational Notes

- `AGENTS.md` deve ser atualizado após Fase 1 para documentar `SourceAttemptResult` e os novos gap kinds.
- `AGENTS.md` deve mencionar `SourceMemoryStore` e onde o diretório `_source_memory/` fica após Fase 2.
- O calibrador (Unit 9) deve ter seu uso documentado no README em seção "Ferramentas offline".
- Nenhuma mudança de variável de ambiente é necessária nas Fases 1–3.

## Sources & References

- Código estudado: `src/peopledd/runtime/adaptive_policy.py`, `circuit_breaker.py`, `phase_assessment.py`, `context.py`, `adaptive_models.py`
- `src/peopledd/services/ri_scraper.py`, `governance_fusion_judge.py`, `governance_observation_builder.py`
- `src/peopledd/nodes/n0_entity_resolution.py`, `n1_governance_ingestion.py`, `n2_person_resolution.py`
- Análise de lacunas: `build_resolved_snapshot` não materializa committees; n2 usa reconciliação não fusão por design; `_ingest_current` silencia todas as exceções
- `docs/INTEGRATION_EXA_PEOPLE_SEARCH.md` — contexto de fontes de pessoas e Exa
