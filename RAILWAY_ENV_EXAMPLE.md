# Railway Raw Environment Variables Editor Example

Aqui está como copiar e colar as variáveis no **Raw Editor** do Railway Dashboard:

## Passo-a-passo:

1. Abra Railway Dashboard → seu projeto → Service (peopledd-api)
2. Clique em **Variables** (ou "Settings" → "Environment Variables")
3. Clique no ícone **Raw Editor** (ícone de texto/código)
4. **Cole o conteúdo abaixo** na caixa de texto

---

## Template Raw (Copiar e Colar)

```
# === API Server Configuration ===
PORT=8000
HOST=0.0.0.0
PEOPLEDD_OUTPUT_DIR=/app/runs

# === Postgres job queue (REQUIRED for POST /jobs) ===
# Railway: add Postgres plugin and reference its DATABASE_URL on API + Worker services
DATABASE_URL=postgresql://user:pass@host:5432/dbname?sslmode=require

# === API auth (set in production; OpenWebUI pipe sends Bearer + X-User-Subject) ===
PEOPLEDD_API_KEY=long-random-secret
PEOPLEDD_MAX_CONCURRENT_GLOBAL=12
PEOPLEDD_MAX_CONCURRENT_PER_USER=2

# Worker service (same image, start: python -m peopledd.worker)
PEOPLEDD_WORKER_POLL_SEC=2
PEOPLEDD_STALE_RUNNING_MINUTES=60

# === Core LLM (REQUIRED - get from openai.com) ===
OPENAI_API_KEY=sk-proj-your-key-here
OPENAI_MODEL=gpt-4o-mini
OPENAI_MODEL_MINI=gpt-4o-mini
OPENAI_MARKET_PULSE_MODEL=gpt-4o-mini

# === Search & Discovery APIs ===
EXA_API_KEY=your-exa-api-key-here
SEARXNG_URL=http://searxng-instance.com

# === Data Enrichment ===
HARVEST_API_KEY=your-harvest-key-here
JINA_API_KEY=your-jina-key-here

# === Web Scraping & Rendering ===
BROWSERLESS_ENDPOINT=wss://your-browserless-endpoint.com
BROWSERLESS_TOKEN=your-browserless-token

# === Optional: Alternative LLM ===
PERPLEXITY_API_KEY=your-perplexity-key-here

# === Optional: Search Backends ===
SERPER_API_KEY=your-serper-key-here
```

---

## Passo 4: Substituir os valores

| Variável | Valor | Onde obter |
|----------|-------|-----------|
| `OPENAI_API_KEY` | `sk-proj-...` | https://platform.openai.com/api-keys |
| `EXA_API_KEY` | Your API key | https://api.exa.ai/ |
| `HARVEST_API_KEY` | Your API key | Harvest.ai dashboard |
| `JINA_API_KEY` | Your API key | https://jina.ai/ |
| `BROWSERLESS_*` | Your endpoint/token | Seu servidor Browserless ou Browserless cloud |
| `PERPLEXITY_API_KEY` | Your API key | https://www.perplexity.ai/api |

---

## Exemplo COMPLETO (com valores fictícios para referência)

```
PORT=8000
HOST=0.0.0.0
PEOPLEDD_OUTPUT_DIR=/app/runs
OPENAI_API_KEY=sk-proj-abc123def456ghi789jkl0mn1op2qr
OPENAI_MODEL=gpt-4o-mini
OPENAI_MODEL_MINI=gpt-4o-mini
OPENAI_MARKET_PULSE_MODEL=gpt-4o-mini
EXA_API_KEY=exa-1234567890abcdefghijklmnopqrs
SEARXNG_URL=https://searxng.yourdomain.com
HARVEST_API_KEY=harvest_key_abc123def456
JINA_API_KEY=jina_1234567890abcdefghijk
BROWSERLESS_ENDPOINT=wss://browserless.yourdomain.com
BROWSERLESS_TOKEN=token_abc123def456
PERPLEXITY_API_KEY=pplx-1234567890abcdefghijk
SERPER_API_KEY=serper_1234567890abcdefghijk
```

---

## Dicas Importantes

1. **Variáveis OBRIGATÓRIAS (mínimo):**
   - `OPENAI_API_KEY` ← sem isso, análises com LLM falham
   - `PORT`, `HOST`, `PEOPLEDD_OUTPUT_DIR` ← já preenchidas

2. **Variáveis RECOMENDADAS:**
   - `EXA_API_KEY` ← para buscas web de qualidade
   - `HARVEST_API_KEY` ← para enriquecimento de pessoas
   - `BROWSERLESS_*` ← para scraping de JS-heavy sites

3. **Variáveis OPCIONAIS:**
   - Tudo em "Optional: Alternative LLM" e "Optional: Search Backends"
   - Se não configuradas, o sistema usa fallbacks

4. **Após colar:**
   - Clique **Save** (ou confirme)
   - Railway **auto-redeploy** da aplicação com as novas env vars
   - Verifique logs: Service → Logs

---

## Validação

Teste se as variáveis foram aplicadas:

```bash
curl https://your-railway-domain.app/health

# Resposta esperada:
# {"status":"healthy","version":"0.1.0","timestamp":"2025-01-15T..."}
```

Se receber erro de `OPENAI_API_KEY`, significa que a env var não foi salva corretamente.

---

## FAQ

**P: Posso deixar algumas em branco?**  
A: Sim, exceto `OPENAI_API_KEY`. Se deixar em branco, o sistema tenta usar fallbacks.

**P: Onde vejo as env vars depois de salvar?**  
A: Railway → Service → Variables → clique "Raw Editor" de novo para confirmar.

**P: Preciso fazer redeploy manual?**  
A: Não! Railway faz auto-redeploy quando você salva as env vars.

**P: Como reverto uma env var errada?**  
A: Volte em Variables → Raw Editor → remova/corrija → Save.
