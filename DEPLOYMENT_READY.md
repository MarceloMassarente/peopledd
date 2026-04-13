# System Ready for Railway Deployment

## Summary

The peopledd system is now fully prepared for deployment on Railway with REST API endpoints accessible to external tools.

## What Was Added

### 1. **REST API Layer** (`src/peopledd/api.py`)
- FastAPI server with async/background task support
- Endpoints for:
  - `POST /analyze` — Start new company analysis
  - `GET /runs/{id}/status` — Check analysis progress
  - `GET /runs/{id}/result` — Get full analysis results
  - `GET /runs/{id}/brief` — Get due diligence brief
  - `GET /runs/{id}/diff/{id2}` — Compare two runs
  - `GET /runs` — List recent analyses
  - `GET /health` — Health check (for monitoring)
  - `GET /openapi.json` — OpenAPI schema (for tool integration)

### 2. **Containerization**
- **Dockerfile**: Multi-stage build, Python 3.11-slim, health checks
- **docker-compose.yml**: Local development setup with volumes
- **Procfile**: Railway start command
- **railway.toml**: Railway service configuration

### 3. **Documentation**
- **API.md**: Comprehensive REST API guide with examples (cURL, Python, JavaScript)
- **RAILWAY.md**: Step-by-step deployment guide for Railway

### 4. **Examples & Tools**
- **examples_api.py**: Production-ready Python client demonstrating async polling
- **.env.example**: Updated with Railway deployment guidance

### 5. **Dependencies**
- Updated `pyproject.toml` to include `fastapi>=0.104.0` and `uvicorn>=0.24.0`

## Quick Start

### Local Testing

```bash
# Option 1: Run directly
python -m peopledd.api

# Option 2: Use Docker Compose
docker-compose up
```

Test endpoint:
```bash
curl http://localhost:8000/health
```

### Railway Deployment

1. Connect GitHub repo to Railway
2. Set environment variables (OPENAI_API_KEY, etc.)
3. Create persistent volume at `/app/runs`
4. Deploy — Railway auto-detects Dockerfile

Result: API accessible at `https://<your-service>.railway.app`

## Integration with External Tools

Any external tool can now:

1. **Discover endpoints**: `GET /openapi.json` (OpenAPI 3.0 schema)
2. **Start analysis**: `POST /analyze` with company details
3. **Poll status**: `GET /runs/{id}/status`
4. **Fetch results**: `GET /runs/{id}/result` or `/brief`

### Example Integration: n8n Workflow

```
HTTP Request → POST /analyze
    ↓
Polling → GET /runs/{id}/status
    ↓
HTTP Request → GET /runs/{id}/result
    ↓
Continue workflow with results
```

## Architecture

```
External Tool
     ↓
HTTP Request (REST API)
     ↓
FastAPI Server (peopledd/api.py)
     ↓
Background Task (asyncio)
     ↓
Pipeline Execution (n0-n9)
     ↓
Artifacts → /app/runs (persistent volume)
     ↓
Results returned via GET endpoints
```

## Key Features

✅ **Async Execution**: Long-running analyses don't block HTTP responses
✅ **Persistent Storage**: Results saved to volume-mounted `/app/runs`
✅ **Health Monitoring**: Railway auto-checks endpoint every 30s
✅ **OpenAPI Discovery**: Clients can auto-generate code from schema
✅ **CORS Enabled**: Cross-origin requests for web frontends
✅ **Scalable**: Stateless design allows horizontal scaling
✅ **Production Ready**: Error handling, logging, type hints

## Monitoring & Logs

- **Railway Dashboard**: Real-time logs, CPU/memory, deployment status
- **Health Endpoint**: `/health` returns service status
- **Run Summaries**: `run_summary.json` with execution details

## Cost & Scaling

| Scale | Replicas | Memory | Storage | Est. Cost |
|-------|----------|--------|---------|-----------|
| Dev | 1 | 512MB | 1GB | ~$6/mo |
| Small Prod | 1 | 1GB | 5GB | ~$15/mo |
| Med Prod | 2 | 2GB each | 10GB | ~$50/mo |
| Large Prod | 3+ | 4GB+ each | 20GB+ | $100+/mo |

## Next Steps

1. **Test Locally**: `docker-compose up && python examples_api.py single`
2. **Deploy to Railway**: Follow RAILWAY.md step-by-step
3. **Integrate External Tools**: Use OpenAPI schema at `/openapi.json`
4. **Monitor**: Check Railway logs for errors/performance
5. **Scale**: Add replicas/memory as needed

## Files Changed/Created

| File | Purpose |
|------|---------|
| `src/peopledd/api.py` | FastAPI REST server |
| `Dockerfile` | Container image definition |
| `Procfile` | Railway start command |
| `railway.toml` | Railway configuration |
| `docker-compose.yml` | Local dev environment |
| `API.md` | REST API documentation |
| `RAILWAY.md` | Deployment guide |
| `examples_api.py` | Example client |
| `pyproject.toml` | Updated dependencies |
| `.env.example` | Updated with Railway guidance |

## Commit

**Commit Hash**: b3f280f
**Message**: feat: REST API wrapper for Railway deployment

All changes pushed to `main` branch.

---

**System is now ready for production deployment on Railway with full REST API integration for external tools.**
