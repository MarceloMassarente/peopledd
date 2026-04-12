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
- CLI com **`--describe-run`** (contrato JSON), **`--dry-run`** (plano sem rede), **`--input-json`**, **`--list-runs`**, **`--show-run`**, **`--diff-runs`**.
- **`dd_brief.json`** por corrida bem-sucedida (resumo orientado a due diligence).
- Em falha de pipeline ou de escrita de artefatos, **`run_summary.json`** com `status: "error"` quando possível.

> Os conectores externos evoluem por gates; variáveis de ambiente documentadas abaixo e em `peopledd --describe-run`.

**Carta de missão e limites:** [docs/DUE_DILIGENCE_CHARTER.md](docs/DUE_DILIGENCE_CHARTER.md). Mapeamento de rubrica (template): [docs/NIOSS_MAPPING.md](docs/NIOSS_MAPPING.md). Gold set: [docs/GOLD_SET.md](docs/GOLD_SET.md).

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
| `--describe-run` | Imprime JSON: estágios do pipeline, lista de artefatos por `output_mode`, dicas de variáveis de ambiente e JSON Schema de `InputPayload`. Não exige `--company-name`. Se combinar com `--dry-run`, só corre **`--describe-run`**. |
| `--dry-run` | Valida `--output-dir` (gravável), imprime plano em texto. Sem rede nem LLM. |
| `--input-json PATH` | Carrega `InputPayload` a partir de JSON; flags `--no-*`, `--company-name`, `--country`, `--output-mode`, etc., redefinem campos quando passados na linha de comandos (ver [AGENTS.md](AGENTS.md)). |
| `--list-runs` | Lista `run_id` sob `--output-dir` com `run_summary.json` ou `run_log.json` (mais recentes primeiro). |
| `--show-run RUN_ID` | Imprime `run_summary.json` dessa corrida. |
| `--diff-runs A B` | Compara duas corridas (usa `final_report.json` se existir; senão `run_summary.json`); saída JSON. |

```bash
python -m peopledd.cli --describe-run
python -m peopledd.cli --company-name "Acme SA" --dry-run --output-dir run --output-mode json
python -m peopledd.cli --input-json examples/input.sample.json --dry-run --output-dir run
python -m peopledd.cli --output-dir run --list-runs
python -m peopledd.cli --output-dir run --show-run <uuid>
python -m peopledd.cli --output-dir run --diff-runs <uuid_a> <uuid_b>
```

### Saída da CLI após uma corrida real

- **stdout**: em `--output-mode report`, o Markdown do relatório; em `json` ou `both`, o JSON completo do `FinalReport` (indentado).
- **stderr**: resumo operacional (`run_id`, pasta da corrida, caminhos de `run_summary.json`, `dd_brief.json`, `final_report.json`, `service_level`, motivo de skip do market pulse quando existir, chamadas LLM contabilizadas e skips de orçamento).

### Modos de artefatos (`--output-mode`)

- **`both`**: todos os JSON intermediários + `final_report.json` + `final_report.md` (quando aplicável).
- **`json`**: JSON apenas (sem `final_report.md`).
- **`report`**: conjunto reduzido (entrada, trace, log, degradação, relatório final JSON + MD).

Em **todos** os modos, após sucesso, são escritos **`run_summary.json`** e **`dd_brief.json`**. Valores inválidos de `output_mode` falham com erro (não há fallback silencioso para “escrever tudo”).

A lista exata por modo é definida em `src/peopledd/runtime/artifact_policy.py` e espelhada em `peopledd --describe-run`.

### Validação do diretório de saída

O **`run_pipeline` / `run_pipeline_graph`** valida `output_dir` antes de criar `RunContext`. A CLI valida também em **`--dry-run`**; numa corrida real a validação ocorre dentro do pipeline (evita dupla sonda ao disco). Falhas levantam `OutputDirectoryError`; na CLI o processo termina com código **2**.

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
  cli.py                 # Entrada CLI (run, describe-run, dry-run, inspect, diff)
  orchestrator.py        # Facade run_pipeline
  runtime/
    graph_runner.py      # Execução n0–n9, artefatos, telemetria
    context.py           # RunContext, orçamento LLM, trace
    artifact_policy.py   # Artefatos por output_mode, validate_output_mode
    run_metadata.py      # run_summary, dd_brief, describe_run, erro resumido
    run_inspect.py       # list_runs, read_run_summary, diff_runs
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
