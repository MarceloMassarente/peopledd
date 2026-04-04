# Integração: `exa_people_search_tool.py` no Peopledd

## Resumo Executivo

A ferramenta `exa_people_search_tool.py` (Open WebUI) implementa um **pipeline Exa Search especializado em perfis (`category=people`)** com:
- Query principal + **variantes** (2–3 por LLM)
- **Gatekeeper** (senioridade, keywords, domínio)
- **Reranking** opcional (Voyage ou LLM juiz)
- Telemetria detalhada (`scoring_plan_json`)

No peopledd atual (n2), usamos:
1. **Harvest** → LinkedIn via API
2. **Fallback Exa** → URL discovery `site:linkedin.com/in`

**Proposta:** Adicionar uma **terceira estratégia** (ou substituir a #2) com **Exa people + variantes + scoring**, especialmente para:
- Homónimos / ambiguidade alta
- Histórico profissional profundo (validação de mandato)
- Conselheiros em múltiplas empresas / posições prévias

---

## 1. Arquitetura Atual (n2_person_resolution)

```
Entrada: GovernanceReconciliation (lista de nomes de conselheiros/executivos)
         ↓
    Para cada {name, company}:
      1. Harvest.search_by_name(name, company)
         ├─ Se achado → candidatos Harvest (LinkedIn URLs via API)
         ├─ Confiança: ~0.8–0.95
         └─ Provider: "harvest"
      
      2. Se Harvest vazio + SearchOrchestrator disponível:
         └─ person_sourcing.linkedin_profile_urls(name, company)
            ├─ Query: "name company site:linkedin.com/in"
            ├─ Via Exa ou SearXNG
            ├─ Confiança: ~0.52 (reduzida por ser web discovery)
            └─ Provider: "exa_web"

      3. Se ainda vazio:
         └─ PersonResolution(status=NOT_FOUND, confidence=0.2)
         
      ↓
   Saída: PersonResolution[] com matched_profiles[] + confidence
```

**Limitações:**
- Query #2 é genérica (`site:linkedin.com/in`)
- Sem variantes de sintaxe/PT-EN
- Sem reranking semântico (usa ranking Exa puro)
- Sem validação de senioridade/domínio no scoring

---

## 2. A Tool `exa_people_search_tool.py` em Detalhe

### Inputs
```python
async def exa_search_people_profiles(
    query: str,                                # "CEO Brazil infrastructure"
    query_variants: Optional[List[str]],       # ["Diretor Executivo Brasil...", "CXO energy..."]
    highlight_query: Optional[str],            # "IPO, M&A, board member"
    
    min_seniority: Optional[str],              # "C_LEVEL" / "VP" / "DIRECTOR"
    max_seniority: Optional[str],              # "BOARD"
    min_relevance_score: Optional[float],      # 0.6 (descarta LOW)
    
    scoring_plan_json: Optional[dict],         # {
                                               #   "role.gate.min_tier_to_pass": "A",
                                               #   "role.tiers.A": ["CEO", "CFO"],
                                               #   "role.tiers.B": ["VP"],
                                               #   "domain.mode": "FLEX",
                                               #   "domain.exact_terms": ["infrastructure"],
                                               # }
    
    max_profiles_to_score: Optional[int],      # 60 (enviar ao LLM)
    RERANKING_MODE: str,                       # "llm" | "voyage" | "none"
    __event_emitter__=None,
) -> str:  # JSON com perfis + telemetria
```

### Outputs (JSON estruturado)
```json
{
  "query_used": "CEO Brazil infrastructure board member",
  "dataset": [
    {
      "url": "https://...",
      "title": "CEO — Infrastructure Company",
      "enriched_data": {
        "current_title": "Chief Executive Officer",
        "current_company": "InfraCompany Brasil",
        "offlimits": false
      }
    },
    ...
  ],
  "stats": {
    "total_before_filter": 45,
    "filtered_out": 12,
    "returned": 33
  }
}
```

### Fluxo Interno
1. **Query paralela**: corre `query` + `query_variants` em paralelo (max concorrência = 3)
2. **Dedup por URL**: agrupa resultados, mantém posição melhor
3. **Gatekeeper**: filtra por keyword exclusões, senioridade mín/máx
4. **Scoring plan**: valida tier de cargo + domínio (PT/EN)
5. **Reranking** (opcional):
   - `"voyage"`: cross-encoder de similaridade
   - `"llm"`: LLM juiz avalia cada perfil → `HIGH|MEDIUM|LOW`
6. **Output**: markdown + JSON estruturado

---

## 3. Cenários de Integração

### Cenário A: **Adapter Fino** (recomendado médio prazo)

**Não importar a tool inteira.** Extrair a **lógica HTTP + contrato de API**:

```
peopledd/src/peopledd/services/exa_people_adapter.py
├─ class ExaPeopleService
│  ├─ __init__(api_key, llm_provider, llm_api_key)
│  ├─ async search_profiles(
│  │     person_name, company,
│  │     role_tier, domain_terms,
│  │     llm_reranking=True
│  │  )
│  └─ _build_query_and_variants(name, company, role_tier)  # LLM-powered
└─ models
   ├─ ExaPeopleSearchRequest  (pydantic)
   ├─ ExaPeopleSearchResult   (pydantic)
   └─ ScoringPlanConfig       (pydantic, para tier/domain)
```

**Integração em n2:**
```python
# n2_person_resolution.py — nova etapa #2.5

elif not candidates and exa_people_service is not None:
    # Tentar Exa people com variantes + scoring
    exa_result = await exa_people_service.search_profiles(
        person_name=name,
        company_name=company_name,
        role_tier=inferred_tier,  # "DIRECTOR" | "VP" | "C_LEVEL"
        domain_terms=extract_sector_keywords(company_name),
    )
    if exa_result.profiles:
        candidates = exa_result.to_harvest_style_candidates()
        candidates_from_sourcing = "exa_people"
```

**Vantagens:**
- Mantém peopledd independente de Open WebUI deps
- Contrato estável (Pydantic models)
- Reutiliza lógica HTTP + query generatio

**Desvantagens:**
- Duplicação parcial de código
- Manutenção de duas code bases
- LLM para variantes + tier inference adicionais

---

### Cenário B: **Integração Lightweight** (curto prazo, desenvolvimento)

Usar a tool em desenvolvimento **com configuração por caminho**:

```python
# .env
EXA_PEOPLE_SEARCH_TOOL_PATH=/path/to/exa_people_search_tool.py
EXA_PEOPLE_SEARCH_ENABLED=true
```

```python
# peopledd/runtime/exa_people_loader.py

def load_exa_people_tool(path: str | None = None) -> Tools | None:
    if not path:
        return None
    try:
        spec = importlib.util.spec_from_file_location("exa_tool", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.Tools()
    except ImportError as e:
        logger.warning("Failed to load exa_people_search_tool: %s", e)
        return None
```

**Integração em n2:**
```python
exa_tool = load_exa_people_tool(os.getenv("EXA_PEOPLE_SEARCH_TOOL_PATH"))

if not candidates and exa_tool is not None:
    result_str = await exa_tool.exa_search_people_profiles(
        query=f"{name} {company_name}",
        query_variants=[...],  # LLM-generated
        scoring_plan_json={
            "role.gate.min_tier_to_pass": inferred_tier,
            "role.tiers.A": ["CEO", "CFO", "COO"],
            ...
        }
    )
    candidates = parse_exa_tool_json_to_harvest_style(result_str)
    candidates_from_sourcing = "exa_people"
```

**Vantagens:**
- Rápido de testar (sem refactoring)
- Usa a tool como-está
- Impacto mínimo no peopledd core

**Desvantagens:**
- Acoplamento a Open WebUI deps (httpx, pydantic, etc.)
- Path absoluto quebra em CI/CD
- Sem type hints (carregamento dinâmico)

---

### Cenário C: **HTTP Wrapper** (longo prazo, produção)

Hospedar a tool como **micro-serviço** (FastAPI):

```
Serviço: exa-people-service (Docker)
├─ POST /search
│  ├─ Input: { query, query_variants, scoring_plan_json, ... }
│  └─ Output: { dataset: [...], stats: {...} }
├─ GET /health
└─ GET /config
```

Peopledd chama via HTTP:
```python
class ExaPeopleHttpClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
    
    async def search_profiles(self, req: ExaPeopleSearchRequest) -> ExaPeopleSearchResult:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}/search", json=req.model_dump())
            return ExaPeopleSearchResult.model_validate(resp.json())
```

**Vantagens:**
- Desacoplamento total
- Escalabilidade (múltiplas replicas Exa People Service)
- Versioning independente

**Desvantagens:**
- Infraestrutura adicional
- Latência de rede
- Dependência de serviço externo

---

## 4. Fluxo Proposto em Detalhe

### Decisão: Adapter Fino (Cenário A) + Telemetria Adaptive

```
n2_person_resolution.py
├─ Para cada {name, company}:
│  │
│  ├─ [#1] Harvest.search_by_name(name, company)
│  │   └─ Se achado → candidatos
│  │
│  ├─ [#2 — NOVO] Se Harvest vazio + exa_people_service:
│  │   ├─ Infer role tier:  conselheiro → "BOARD", exec → "C_LEVEL"
│  │   ├─ Generate query_variants via LLM (usando exa_modular Skill 01)
│  │   ├─ call exa_people_service.search_profiles()
│  │   ├─ record SearchAttemptRecord (adaptive telemetry)
│  │   └─ Se achado → candidatos com confidence cap ~0.65
│  │
│  ├─ [#3 — Fallback] Se Exa people vazio + orchestrator.exa:
│  │   ├─ LinkedIn URL discovery (query genérica)
│  │   └─ Confidence ~0.52
│  │
│  └─ [#4] Se ainda vazio → NOT_FOUND
│
└─ Saída: PersonResolution[] com mix de providers
```

### Novo Contrato: `ExaPeopleService`

```python
# peopledd/services/exa_people_service.py

class ExaPeopleConfig(BaseModel):
    api_key: str
    llm_provider: str  # "openai" | "gemini" | "anthropic"
    llm_api_key: str
    llm_model: str  # "gpt-4o-mini", etc.
    reranking_mode: Literal["llm", "voyage", "none"] = "llm"
    voyage_api_key: Optional[str] = None

class ExaPeopleSearchRequest(BaseModel):
    person_name: str
    company_name: str | None
    role_tier: Literal["DIRECTOR", "VP", "C_LEVEL", "BOARD"]
    domain_terms: list[str]  # ["infrastructure", "energy"]
    min_seniority: str | None = None
    min_relevance_score: float = 0.5
    max_profiles_to_score: int = 60

class ExaPeopleProfile(BaseModel):
    url: str
    title: str
    current_title: str
    current_company: str
    enriched_data: dict[str, Any]
    relevance_score: Optional[float] = None
    llm_classification: Optional[str] = None  # "HIGH" | "MEDIUM" | "LOW"

class ExaPeopleSearchResult(BaseModel):
    query_used: str
    profiles: list[ExaPeopleProfile]
    total_raw: int
    total_filtered: int
    
    def to_harvest_style_candidates(self) -> list[ProfileSearchResult]:
        """Convert to peopledd's existing candidate format."""
        ...

class ExaPeopleService:
    def __init__(self, config: ExaPeopleConfig, run_context: RunContext | None = None):
        self.config = config
        self.ctx = run_context  # Para telemetria adaptive
    
    async def search_profiles(self, req: ExaPeopleSearchRequest) -> ExaPeopleSearchResult:
        """
        1. Generate query + variants via LLM
        2. Call Exa people API with scoring_plan_json
        3. Post-process results
        4. Record telemetry
        5. Return structured result
        """
        attempt_index = self.ctx.search_attempt_counter if self.ctx else 0
        
        # Step 1: Generate variants
        query_variants = await self._generate_query_variants(req)
        
        # Step 2: Build scoring_plan_json
        scoring_plan = self._build_scoring_plan(req)
        
        # Step 3: Call Exa people
        exa_result = await self._call_exa_people(
            query=f"{req.person_name} {req.company_name or ''}",
            query_variants=query_variants,
            scoring_plan_json=scoring_plan,
            min_relevance_score=req.min_relevance_score,
            max_profiles_to_score=req.max_profiles_to_score,
        )
        
        # Step 4: Record telemetry
        if self.ctx:
            self.ctx.record_search_attempt(
                SearchAttemptRecord(
                    purpose="person_exa_people",
                    attempt_index=attempt_index,
                    escalation_level="variant_rich",
                    exa_num_results_requested=len(exa_result.profiles) * 2,
                    url_count=len(exa_result.profiles),
                    empty_pool=len(exa_result.profiles) == 0,
                    topic_excerpt=f"{req.person_name} @ {req.company_name}",
                )
            )
        
        return exa_result
    
    async def _generate_query_variants(self, req: ExaPeopleSearchRequest) -> list[str]:
        """Use LLM (via exa_modular Skill 01) to generate variants."""
        # Could use ce-brainstorm or direct LLM call
        # Returns: ["Diretor Executivo...", "CXO energy..."]
        ...
    
    def _build_scoring_plan(self, req: ExaPeopleSearchRequest) -> dict[str, Any]:
        """Build scoring_plan_json based on role_tier + domain_terms."""
        tier_map = {
            "BOARD": ("A", ["Chairman", "Board Member", "Independent Director"]),
            "C_LEVEL": ("A", ["CEO", "CFO", "COO", "CTO", "General Director"]),
            "VP": ("B", ["VP", "SVP", "EVP", "Head of"]),
            "DIRECTOR": ("C", ["Director", "Manager", "Lead"]),
        }
        tier, titles = tier_map.get(req.role_tier, ("B", []))
        
        return {
            "role.gate.min_tier_to_pass": tier,
            "role.gate.gate_mode": "CURRENT_OR_PAST",
            "role.tiers.A": titles,
            "role.tiers.B": ["VP", "SVP", "Head"],
            "role.tiers.C": ["Manager", "Director"],
            "domain.mode": "FLEX",
            "domain.exact_terms": req.domain_terms,
        }
    
    async def _call_exa_people(self, ...) -> ExaPeopleSearchResult:
        """POST to Exa people API or call local tool."""
        # Using httpx or direct Exa SDK call
        ...
```

### Integração em n2

```python
# n2_person_resolution.py

def run(
    reconciled: GovernanceReconciliation,
    harvest: HarvestAdapter,
    company_name: str | None = None,
    search_orchestrator: SearchOrchestrator | None = None,
    exa_people_service: ExaPeopleService | None = None,  # NEW
    use_harvest: bool = True,
    person_search_params: PersonSearchParams | None = None,
) -> list[PersonResolution]:
    ...
    for attempt_index, name in enumerate(sorted_people):
        ...
        
        # #1 Harvest
        if use_harvest:
            try:
                outcome = harvest.search_by_name(name=name, company=company_name)
                candidates = outcome.candidates
            except Exception as e:
                ...
        else:
            candidates = []
        
        # #2 Exa People (NEW, prioritário sobre LinkedIn discovery)
        if not candidates and exa_people_service is not None:
            role_tier = infer_role_tier(reconciled, name)  # BOARD | C_LEVEL | VP
            domain_terms = extract_domain_keywords(company_name)
            
            try:
                epa_result = asyncio.run(
                    exa_people_service.search_profiles(
                        ExaPeopleSearchRequest(
                            person_name=name,
                            company_name=company_name,
                            role_tier=role_tier,
                            domain_terms=domain_terms,
                            min_relevance_score=0.5,
                        )
                    )
                )
                if epa_result.profiles:
                    candidates = epa_result.to_harvest_style_candidates()
                    candidates_from_sourcing = "exa_people"
                    logger.info(
                        "[n2] Exa People found %d profiles for '%s'",
                        len(candidates), name
                    )
            except Exception as e:
                logger.warning("[n2] Exa People search failed: %s", e)
        
        # #3 Fallback: LinkedIn URL discovery (genérico)
        if not candidates and search_orchestrator is not None:
            urls = person_sourcing.linkedin_profile_urls(...)
            if urls:
                candidates = person_sourcing.harvest_style_results_from_urls(urls[:5], name, company_name)
                candidates_from_sourcing = "exa_web"
        
        # Resto do n2...
```

---

## 5. Impacto na Telemetria Adaptive

### SearchAttemptRecord (existente)
```python
@dataclass
class SearchAttemptRecord:
    purpose: str  # "person_linkedin" | "person_exa_people" (NEW)
    attempt_index: int
    escalation_level: str  # "initial", "variant_rich" (NEW)
    searxng_queries_used: int
    exa_num_results_requested: int
    url_count: int
    empty_pool: bool
    topic_excerpt: str
```

### Novo campo em PipelineTelemetry
```python
class PipelineTelemetry(BaseModel):
    ...
    search_attempts: list[SearchAttemptRecord]  # (já existe)
    adaptive_decisions: list[AdaptiveDecisionRecord]  # (já existe)
    # NEW: track Exa People vs Exa LinkedIn discovery
    exa_people_used_for_names: set[str]  # ["John Doe", ...]
    exa_people_match_count: int  # Total de hits
```

### Policy Impact
```python
# adaptive_policy.py

class DefaultAdaptivePolicy:
    def assess_person_resolution(self, assessment: PhaseAssessment) -> AdaptiveAction | None:
        """
        Decide if retry/escalate person resolution based on:
        - Ambiguous resolutions (2+ candidates, similarity ~0.75)
        - Empty resolutions after Harvest
        - High LLM cost in other phases
        """
        unresolved_count = sum(
            1 for p in assessment.person_resolutions
            if p.resolution_status in [ResolutionStatus.NOT_FOUND, ResolutionStatus.AMBIGUOUS]
        )
        
        if unresolved_count > 0.3 * len(assessment.person_resolutions):
            if not any(a.purpose == "person_exa_people" for a in assessment.search_attempts):
                # Trigger Exa People if not yet used
                return AdaptiveAction(
                    action="escalate_person_resolution",
                    reason="High unresolved ratio; try Exa People",
                    target_node="n2_with_exa_people",
                )
        
        return None
```

---

## 6. Comparação: Harvest vs Exa LinkedIn vs Exa People

| Dimensão | Harvest | Exa LinkedIn Discovery | Exa People (NEW) |
|----------|---------|------------------------|------------------|
| **API** | LinkedIn via SearchAuth | Exa generic + site: filter | Exa category=people |
| **Query** | Estruturada (firstName, lastName, company) | "name company site:linkedin.com/in" | "CEO Brasil infrastructure" + 2–3 variantes |
| **Accuracy** | ~85–95% (dados LinkedIn API) | ~60–75% (web discovery) | ~70–85% (Exa people índice) |
| **Work History** | Superficial (headlines) | Não | Profundo (entities.workHistory) |
| **Confiança** | 0.80–0.95 | 0.52 | 0.60–0.80 (com Exa people) |
| **Reranking** | Ranking LI nativo | Ranking Exa genérico | Voyage ou LLM juiz |
| **Latência** | ~200ms | ~500ms | ~1–2s (com LLM rerank) |
| **Custo** | Alto (API LinkedIn) | Médio (Exa calls) | Médio–Alto (Exa + LLM tokens) |
| **Homónimos** | Boa dedup (perfil único) | Fraca (URLs genéricas) | Boa (scoring+LLM) |
| **Board Members** | Varável (nem todos no LI) | Fraca (site: generic) | Excelente (domínio + tier) |

---

## 7. Implementação: Roadmap

### Fase 1: Adapter Interface (1–2 semanas)
1. Define `peopledd/services/exa_people_service.py` (Pydantic models + interface)
2. Stub implementation (mock responses para testes)
3. Integrate em n2 (nova etapa entre Harvest e LinkedIn discovery)
4. Testes unitários (sem chamadas Exa reais)

### Fase 2: HTTP Integration (1 semana)
1. Implement `ExaPeopleService._call_exa_people()` via httpx POST
2. Chamadas reais a Exa API (category=people)
3. Parse JSON result → Pydantic models
4. Tests com fixture responses

### Fase 3: LLM Query Generation (1 semana)
1. `ExaPeopleService._generate_query_variants()` via LLM
2. Integração com exa_modular prompts (Skill 01)
3. Role tier inference (conselheiro → "BOARD", etc.)
4. Telemetria de LLM tokens

### Fase 4: Adaptive Policy (1 semana)
1. `DefaultAdaptivePolicy.assess_person_resolution()` novo
2. Decide when to trigger Exa People retry
3. E2E tests com SearchAttemptRecord telemetry

### Fase 5: Production Hardening (1 semana)
1. Rate limit handling (circuit breaker)
2. Fallback cascades (Exa People → LinkedIn discovery)
3. Monitoring + alerting
4. Documentation + ONBOARDING.md update

---

## 8. Riscos & Mitigações

| Risco | Impacto | Mitigação |
|-------|--------|----------|
| Custo Exa + LLM (variantes + rerank) | Alto (budget LLM) | `max_profiles_to_score` default ~20; circuit breaker; adaptive policy decide |
| Latência 1–2s por pessoa | Alto (n2 é sync today) | Batch queries paralelo; async/await; telemetria para alertar se > threshold |
| `scoring_plan_json` mal formado | Médio (tool falha se JSON inválido) | Validação Pydantic; unit tests per tier; default templates |
| Breaking change em Exa API | Médio (category=people é estável) | Mock tests; versioning de ExaPeopleConfig |
| Dependency hell (httpx, pydantic versions) | Médio | Adapter separado; requirements.txt isolado; CI tests |
| Overhead de LLM para query_variants | Alto (pode não valer para nomes simples) | Only use variantes se name ambiguo ou person_tier=BOARD |

---

## 9. Alternativas Consideradas

### Alternativa 1: Usar Exa people como fallback genérico (sem variantes/LLM)
**Pros:** Simples, sem LLM overhead
**Cons:** Perde 60% do recall (sem variantes PT-EN); sem scoring específico
**Rejeição:** Não alinha com objetivo de rigor

### Alternativa 2: Hardcode scoring_plan_json per tier
**Pros:** Sem LLM, rápido
**Cons:** Inflexível; não captura nuances de domínio
**Possível fusão:** Tier default + override por campanha

### Alternativa 3: Substituir completamente Harvest por Exa People
**Pros:** Único source of truth
**Cons:** Harvest tem 90%+ accuracy; Exa people é complementar, não substituto
**Rejeição:** Risco de queda de recall

**Decisão:** Implementar como **3ª opção (após Harvest, antes de LinkedIn discovery)**, mantendo todos os 3 canais.

---

## 10. Acceptance Criteria

- [ ] `ExaPeopleService` class implementado com async search_profiles()
- [ ] n2_person_resolution integrado (3 canais: Harvest → Exa People → LinkedIn discovery)
- [ ] Telemetria: SearchAttemptRecord.purpose="person_exa_people" capturado
- [ ] E2E test: "ambiguous CEO + domain terms → Exa People match > Harvest"
- [ ] AdaptivePolicy responde a unresolved_count alto
- [ ] Docs: ONBOARDING.md + README sections atualizados
- [ ] CI/CD verde (testes + linting)
- [ ] Rate limit + circuit breaker funcionando
- [ ] LLM tokens capped (máximo X por run)

---

## Próximos Passos

1. **Escolher implementação:** Adapter Fino (A) vs Lightweight (B) vs HTTP Wrapper (C)
   - Recomendação: **Adapter Fino** (A) com fallback opcional para carregamento dinâmico (B) em dev
2. **Priorizar query variant generation** via LLM (usar ce-brainstorm ou direct call)
3. **Mock Exa people responses** para testes unitários
4. **Estimar overhead:** tokens LLM, latência n2, memória
