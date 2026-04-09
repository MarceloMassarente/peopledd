# peopledd

Pipeline de referência para o **SPEC v1.1 — Company Organization & Governance X-ray**.

## O que está implementado

- Estrutura completa do pipeline `n0` a `n9` (inclui `n1b` reconciliação e `n1c` fusão semântica multi-fonte).
- Modelos Pydantic para entrada (`InputPayload`), saídas intermediárias e `FinalReport`.
- Dual-track de governança (`formal` CVM + `current` RI) e reconciliação explícita.
- Classificação `holding|opco|subsidiary|mixed|unknown`.
- Service levels `SL1..SL5` e matriz de degradação explícita.
- Scoring de cobertura com normalização por tamanho do órgão.
- Evidence pack auditável e **persistência de artefatos por execução** sob `OUTPUT_DIR/<run_id>/`.
- **Market pulse** (mídia pública) após `n4`, com telemetria e integração em `n8`/`n9`.
- CLI com **`--describe-run`** (contrato JSON), **`--dry-run`** (plano sem rede) e validação antecipada de diretório de saída.

> Os conectores externos evoluem por gates; variáveis de ambiente documentadas abaixo e em `peopledd --describe-run`.

Para detalhes de nós, toggles e convenções de teste, veja **[AGENTS.md](AGENTS.md)**.

## Dependências

O pacote roda com as dependências declaradas em `pyproject.toml` (PyPI). Para caminhos que chamam OpenAI (estratégia, fusão `n1c`, market pulse, RI/scraper auxiliares, etc.), instale o extra:

```bash
pip install -e ".[strategy]"
```

Isso adiciona o pacote `openai`; em runtime costuma exigir `OPENAI_API_KEY` onde esses caminhos estão ativos.

## Executar

Corrida completa (artefatos em `run/<uuid>/` por omissão):

```bash
python -m peopledd.cli --company-name "Itaú Unibanco" --output-dir run --output-mode both
```

Ou via script instalado:

```bash
peopledd --company-name "Itaú Unibanco" --output-dir run --output-mode both
```

Em automação, use **`--output-dir` absoluto** para não depender do diretório de trabalho do processo.

### Planejamento e contrato (sem rede)

| Flag | Efeito |
|------|--------|
| `--describe-run` | Imprime JSON: estágios do pipeline, lista de artefatos por `output_mode`, dicas de variáveis de ambiente e JSON Schema de `InputPayload`. Não exige `--company-name`. |
| `--dry-run` | Cria/valida `--output-dir` (tem de ser gravável), imprime plano em texto (flags, estágios, ficheiros esperados). Sem chamadas de rede nem LLM. |

```bash
python -m peopledd.cli --describe-run
python -m peopledd.cli --company-name "Acme SA" --dry-run --output-dir run --output-mode json
```

### Saída da CLI após uma corrida real

- **stdout**: em `--output-mode report`, o Markdown do relatório; em `json` ou `both`, o JSON completo do `FinalReport` (indentado).
- **stderr**: resumo operacional (`run_id`, pasta da corrida, caminhos de `run_summary.json` / `final_report.json`, `service_level`, motivo de skip do market pulse quando existir, chamadas LLM contabilizadas e skips de orçamento).

### Modos de artefatos (`--output-mode`)

- **`both`**: todos os JSON intermediários + `final_report.json` + `final_report.md` (quando aplicável).
- **`json`**: JSON apenas (sem `final_report.md`).
- **`report`**: conjunto reduzido (entrada, trace, log, degradação, relatório final JSON + MD).

Em **todos** os modos, após sucesso, é escrito **`run_summary.json`** na pasta da corrida (snapshot compacto: SL, telemetria LLM, pulse, lista de artefatos esperados para o modo).

A lista exata por modo é definida em `src/peopledd/runtime/artifact_policy.py` e espelhada em `peopledd --describe-run`.

### Validação do diretório de saída

Antes de criar `RunContext`, o pipeline verifica se `output_dir` pode ser criado e escrito. Se falhar, lança-se `OutputDirectoryError` (API) ou a CLI termina com erro após `--dry-run` ou no arranque de uma corrida real.

### Exemplo de entrada

- `examples/input.sample.json` — alinhado com `InputPayload` (inclui `prefer_llm`, `use_harvest`, etc.).
- `examples/input.json` — exemplo legado equivalente.

## Variáveis de ambiente (API keys e integrações)

Copie `.env.example` para `.env` e ajuste. Resumo:

| Variável | Uso |
|----------|-----|
| `OPENAI_API_KEY` | LLM: estratégia, juiz de fusão semântica (`n1c`), market pulse, descoberta web de governança, escolha opcional de URL em sourcing de pessoas. |
| `OPENAI_MODEL`, `OPENAI_MODEL_MINI` | Nomes de modelo quando o código resolve via env. |
| `OPENAI_MARKET_PULSE_MODEL` | Modelo dedicado ao market pulse (opcional). |
| `EXA_API_KEY` | Busca Exa (web, empresa/pessoas, fontes do pulse). |
| `SEARXNG_URL` | Instância SearXNG como alternativa/complemento à orquestração de busca. |
| `SERPER_API_KEY` | Backend Serper (Google) quando configurado em `vendor.search`. |
| `PERPLEXITY_API_KEY` | Briefings Sonar opcionais no strategy retriever (`n4`). |
| `HARVEST_API_KEY` | API Harvest (profile search e enriquecimento LinkedIn em `n2`/`n3`). |
| `JINA_API_KEY` | Jina Reader em caminhos de fetch/scrape. |
| `BROWSERLESS_ENDPOINT`, `BROWSERLESS_TOKEN` | Renderização JS via Browserless. |

A lista estruturada com descrições curtas (e o JSON Schema de `InputPayload`) é emitida por **`peopledd --describe-run`**.

## Testes

```bash
pip install -e ".[strategy]"   # se ainda não instalou o extra
pytest
```

Preferir patch dos nós em `peopledd.runtime.graph_runner` para testes offline rápidos (ver `tests/test_pipeline.py`).

## Estrutura do código

```text
src/peopledd/
  cli.py                 # Entrada CLI, --describe-run, --dry-run
  orchestrator.py        # Facade run_pipeline
  runtime/
    graph_runner.py      # Execução n0–n9, artefatos, telemetria
    context.py           # RunContext, orçamento LLM, trace
    artifact_policy.py   # Artefatos por output_mode
    run_metadata.py      # run_summary, describe_run, plano dry-run
  models/
  nodes/
  services/
  vendor/
  utils/
```

## Roadmap sugerido (gates)

1. Gold Set manual (5–8 empresas).
2. Hardening de `n0/n1/n1b`.
3. Benchmark de `useful_coverage` em `n2/n3`.
4. Integração assistida de `n4..n7`.
5. Escala controlada por setor e por modo de empresa.
