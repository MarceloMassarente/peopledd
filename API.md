# peopledd REST API

A REST API for the governance X-ray pipeline. **Production flow:** submit jobs to Postgres (`POST /jobs`), a **separate worker process** executes `run_pipeline`, and clients poll `GET /jobs/{job_id}`. Results are stored as JSON on the job row (and optionally read from `/app/runs` if the filesystem is shared).

## Quick Start

### Local Development (API + Postgres + worker)

```bash
pip install -e ".[strategy]"
docker compose up -d postgres
psql "$DATABASE_URL" -f migrations/001_jobs.sql
# terminal 1
python -m peopledd.worker
# terminal 2
export DATABASE_URL=postgresql://peopledd:peopledd@localhost:5432/peopledd
export PEOPLEDD_API_KEY=devlocal
python -m peopledd.api
```

Or use `docker compose up` (see `docker-compose.yml`) after applying `migrations/001_jobs.sql` once against the compose Postgres instance.

Server starts at `http://localhost:8000`

### Railway Deployment

See `RAILWAY.md` for the full two-service layout (API + Worker + Postgres).

1. Add the **Postgres** plugin and set **`DATABASE_URL`** on both API and Worker services.
2. Run **`migrations/001_jobs.sql`** once against that database (Railway shell or local `psql`).
3. **API service**: default image command runs Uvicorn (`peopledd.api:app`).
4. **Worker service**: same image, start command `python -m peopledd.worker` (or `peopledd-worker`).
5. Set **`PEOPLEDD_API_KEY`**, optional concurrency limits, and LLM/tool keys (`OPENAI_API_KEY`, etc.).
6. Mount **`/app/runs`** on **both** API and Worker if the API should read `final_report.json` from disk when JSON is not yet in the database; otherwise rely on worker-written **`final_report_json`** / **`dd_brief_json`** on the job row.

Access: `https://<your-railway-domain>/`

## Authentication and user scope

When `PEOPLEDD_API_KEY` is set:

- Send `Authorization: Bearer <PEOPLEDD_API_KEY>` on all job and run routes (except `GET /health`).
- Send `X-User-Subject: <stable-id>` on `POST /jobs`; that header is the trusted user identity. Optional `owner_sub` in the JSON body must match the header if you send both.
- List and read endpoints use `X-User-Subject` for filtering.

If `PEOPLEDD_API_KEY` is unset (local dev only), bearer is optional and `POST /jobs` may use `owner_sub` or `X-User-Subject`. If `DATABASE_URL` is set but the API key is not, **`GET /jobs` requires `X-User-Subject`** so anonymous callers cannot list every unscoped job.

**Cancel:** `POST /jobs/{id}/cancel` on a running job is best-effort; the pipeline may still complete. On success, `cancel_requested` is cleared so a completed run does not stay flagged.

**Worker:** With `PEOPLEDD_STALE_RUNNING_MINUTES` (default 60), the worker moves stale `running` rows back to `queued` after a worker crash. Set `0` to disable or raise the value for runs longer than the window.

## Endpoints

### Health Check

**GET** `/health`

**Response:**
```json
{
  "status": "healthy",
  "version": "0.2.0",
  "timestamp": "2026-04-13T10:30:00.000000+00:00",
  "database_configured": true
}
```

---

### Submit job (preferred)

**POST** `/jobs`

Enqueues a pipeline run. Returns immediately with `job_id` and `run_id` (artifact directory name).

Headers: `Authorization` (if `PEOPLEDD_API_KEY` set), `X-User-Subject` (required if API key set).

Body: same fields as the analysis payload below, plus optional `owner_sub` (must match `X-User-Subject` when API key is set) and `client_request_id` (idempotency).

**GET** `/jobs/{job_id}` — status (`queued`, `running`, `succeeded`, `failed`, `cancelled`).

**GET** `/jobs/{job_id}/result` — `FinalReport` JSON (from DB or disk).

**GET** `/jobs/{job_id}/brief` — `dd_brief` JSON.

**POST** `/jobs/{job_id}/cancel` — cancel queued jobs or request cancel on running (best-effort; running pipeline may still finish).

**GET** `/jobs` — list jobs for the current `X-User-Subject`.

**Response (200 OK) for `POST /jobs`:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "queued",
  "message": "queued analysis for Itaú Unibanco",
  "created_at": "2026-04-13T10:30:00.123456+00:00"
}
```

Optional body fields: `owner_sub`, `client_request_id` (unique per owner; duplicate returns the same `job_id`).

---

### Legacy: Start Analysis (dev only)

**POST** `/analyze`

Enabled only when `PEOPLEDD_ALLOW_LEGACY_UNAUTH=true` **and** `PEOPLEDD_API_KEY` is not set. Otherwise returns **404**. Prefer `POST /jobs`.

Trigger a new analysis asynchronously. Returns immediately with a `job_id` and `run_id`.

**Request Body:**
```json
{
  "company_name": "Itaú Unibanco",
  "country": "BR",
  "company_type_hint": "listed",
  "ticker_hint": "ITUB4",
  "analysis_depth": "standard",
  "output_mode": "both",
  "use_harvest": true,
  "prefer_llm": true,
  "use_apify": true,
  "use_browserless": true,
  "allow_manual_resolution": false
}
```

**Response (200 OK):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "queued",
  "message": "legacy queued for Itaú Unibanco",
  "started_at": "2026-04-13T10:30:00.123456+00:00"
}
```

**Fields:**
- `company_name` (string, required): Company name to analyze
- `country` (string, optional): ISO 3166-1 alpha-2 code. Default: "BR"
- `company_type_hint` (string, optional): "auto", "listed", or "private". Default: "auto"
- `ticker_hint` (string, optional): Stock ticker (e.g., "ITUB4")
- `cnpj_hint` (string, optional): Brazilian CNPJ
- `analysis_depth` (string, optional): "standard" or "deep". Default: "standard"
- `output_mode` (string, optional): "json", "report", or "both". Default: "both"
- `use_harvest` (boolean, optional): Enable Harvest for people resolution. Default: true
- `prefer_llm` (boolean, optional): Use LLM for semantic fusion. Default: true
- `use_apify` (boolean, optional): Use Apify where available. Default: true
- `use_browserless` (boolean, optional): Use Browserless for JS rendering. Default: true
- `allow_manual_resolution` (boolean, optional): Allow manual person resolution. Default: false

---

### Check run status (by `run_id`)

**GET** `/runs/{run_id}/status`

When **`DATABASE_URL`** is configured and you use the authenticated contract, this reflects the **job** row for that `run_id` (same artifact directory name as returned by `POST /jobs`).

Requires `Authorization` and `X-User-Subject` when `PEOPLEDD_API_KEY` is set.

**Response:**
```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "running",
  "completed_at": null,
  "error": null
}
```

**Statuses (DB-backed):**
- `queued`, `running` — in progress (`completed_at` is null)
- `succeeded` — finished successfully (`completed_at` set)
- `failed` — pipeline or worker error (`error` may be set)
- `cancelled` — job was cancelled or superseded after cancel

**Example polling loop (Python):**
```python
import requests
import time

run_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
base_url = "http://localhost:8000"
headers = {
    "Authorization": "Bearer YOUR_PEOPLEDD_API_KEY",
    "X-User-Subject": "user-stable-id",
}

while True:
    response = requests.get(f"{base_url}/runs/{run_id}/status", headers=headers)
    status = response.json()
    print(f"Status: {status['status']}")

    if status["status"] in ("succeeded", "failed", "cancelled"):
        break

    time.sleep(5)
```

Without `PEOPLEDD_API_KEY`, local behaviour may still use the filesystem for some paths; prefer **`GET /jobs/{job_id}`** for the canonical status when using Postgres.

---

### 📋 Get Full Result

**GET** `/runs/{run_id}/result`

Fetch the complete analysis result (FinalReport or run_summary).

**Response:**
```json
{
  "entity_resolution": { ... },
  "formal_governance": { ... },
  "current_governance": { ... },
  "degradation_profile": { ... },
  "pipeline_telemetry": { ... }
}
```

Contains all governance, people, committees, and service level information. See peopledd contracts for full schema.

---

### 📝 Get Due Diligence Brief

**GET** `/runs/{run_id}/brief`

Fetch the concise DD brief (dd_brief.json) — summary oriented for due diligence users.

**Response:**
```json
{
  "company_name": "Itaú Unibanco",
  "cnpj": "33.000.167/0001-04",
  "governance_summary": { ... },
  "key_risks": [ ... ],
  "committees": [ ... ]
}
```

---

### 📜 List Recent Runs

**GET** `/runs?limit=50&offset=0`

List completed or running analyses (newest first).

**Query Parameters:**
- `limit` (integer, optional): Max results. Default: 50, max: 500
- `offset` (integer, optional): Pagination offset. Default: 0

**Response** when `PEOPLEDD_API_KEY` is set (jobs for `X-User-Subject`):
```json
{
  "runs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "status": "succeeded",
      "created_at": "2026-04-13T10:30:00.123456+00:00"
    }
  ],
  "count": 1
}
```

**Response** without API key (filesystem-only listing): objects contain `run_id` only.

---

### 🔄 Compare Two Runs

**GET** `/runs/{run_a}/diff/{run_b}`

Compare two analyses (e.g., before/after governance changes).

**Response:**
```json
{
  "comparison": {
    "entity_resolution": { "diff": [...] },
    "formal_governance": { "diff": [...] },
    "current_governance": { "diff": [...] }
  }
}
```

---

### 🔗 OpenAPI Schema

**GET** `/openapi.json`

Fetch OpenAPI 3.0 schema for automated client generation or API documentation tools.

---

## Usage Examples

### cURL

```bash
# Start an analysis
curl -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "company_name": "Itaú Unibanco",
    "country": "BR",
    "company_type_hint": "listed"
  }'

# Check status (substitute run_id)
curl http://localhost:8000/runs/{run_id}/status

# Get results
curl http://localhost:8000/runs/{run_id}/result

# List recent runs
curl "http://localhost:8000/runs?limit=10"
```

### Python

```python
import requests

base_url = "http://localhost:8000"

# Start analysis
response = requests.post(
    f"{base_url}/analyze",
    json={
        "company_name": "Itaú Unibanco",
        "country": "BR",
        "company_type_hint": "listed",
    },
)
run_data = response.json()
run_id = run_data["run_id"]
print(f"Analysis started: {run_id}")

# Poll until complete
while True:
    status_response = requests.get(f"{base_url}/runs/{run_id}/status")
    status = status_response.json()
    print(f"Status: {status['status']}")
    
    if status['status'] == 'completed':
        break
    elif status['status'] == 'error':
        print(f"Error: {status['error']}")
        exit(1)
    
    import time
    time.sleep(5)

# Fetch results
result = requests.get(f"{base_url}/runs/{run_id}/result").json()
brief = requests.get(f"{base_url}/runs/{run_id}/brief").json()
print(f"Entity: {result['entity_resolution']['resolved_name']}")
print(f"Service Level: {result['degradation_profile']['service_level']}")
```

### Node.js / JavaScript

```javascript
const BASE_URL = "http://localhost:8000";

async function analyzeCompany(name) {
  // Start analysis
  const startResponse = await fetch(`${BASE_URL}/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      company_name: name,
      country: "BR",
      company_type_hint: "listed",
    }),
  });

  const { run_id } = await startResponse.json();
  console.log(`Analysis started: ${run_id}`);

  // Poll for completion
  let status = "queued";
  while (status !== "completed" && status !== "error") {
    const statusResponse = await fetch(`${BASE_URL}/runs/${run_id}/status`);
    const data = await statusResponse.json();
    status = data.status;
    console.log(`Status: ${status}`);

    if (status !== "completed" && status !== "error") {
      await new Promise((r) => setTimeout(r, 5000)); // Wait 5 seconds
    }
  }

  if (status === "error") {
    console.error(`Analysis failed: ${data.error}`);
    return null;
  }

  // Fetch result
  const resultResponse = await fetch(`${BASE_URL}/runs/${run_id}/result`);
  return await resultResponse.json();
}

// Usage
const result = await analyzeCompany("Itaú Unibanco");
console.log(result);
```

## Configuration

### Environment Variables

- `PORT` (optional): Server port. Default: 8000
- `HOST` (optional): Server host. Default: 0.0.0.0
- `PEOPLEDD_OUTPUT_DIR` (optional): Directory for run artifacts. Default: /tmp/peopledd_runs
- `OPENAI_API_KEY` (required if using LLM features): OpenAI API key
- `EXA_API_KEY` (optional): Exa search API key
- `HARVEST_API_KEY` (optional): Harvest API key
- Other integration keys as documented in main README.md

### Railway Deployment

Create a `.railwayrc` or set via Railway dashboard:

```toml
[build]
dockerfile = "Dockerfile"

[deploy]
healthcheckPath = "/health"
startCommand = "python -m peopledd.api"
```

**Volumes (recommended):**
- Mount a persistent volume at `/app/runs` to retain artifacts across deployments.

**Networking:**
- Railway automatically exposes the service on `https://<your-service-name>.<railway-project>.railway.app`
- All endpoints are publicly accessible (enable auth/firewall as needed)

## Monitoring & Observability

### Logs

```bash
# Local development (with --reload):
# Logs print to stdout/stderr

# Railway:
# View logs in Railway dashboard → Service → Logs
```

### Metrics

Track via status endpoint:
```bash
curl http://localhost:8000/health
```

### Errors

Run failures are captured in the run_summary.json:
```json
{
  "status": "error",
  "error": "RI scraping timeout after 3 retries",
  "run_id": "..."
}
```

Fetch via `/runs/{run_id}/status` or `/runs/{run_id}/result`.

## Performance Considerations

### Concurrency

- The API server handles concurrent requests via FastAPI/Uvicorn's async model.
- Each analysis runs in a background task; server remains responsive.
- For Railway: use multiple replicas/dyno types for high concurrency.

### Storage

- Artifacts accumulate in `PEOPLEDD_OUTPUT_DIR`.
- Railway ephemeral storage (~512 MB) is insufficient for production.
- **Recommendation**: Mount a persistent volume or use object storage (S3).

### Timeouts

- Analyses typically take **5–30 minutes** depending on data availability and analysis depth.
- HTTP request timeout on client side should be > 30 minutes for deep analyses.
- Recommended: Use async polling (don't wait for HTTP response).

## Security

- **CORS enabled** for all origins (suitable for internal/development use).
- **No authentication** by default (add middleware in production).
- **Sensitive data** (run results) stored on disk; protect via filesystem permissions or volume encryption.

For production:
1. Add API key / Bearer token validation.
2. Restrict CORS origins.
3. Use HTTPS (Railway + custom domain recommended).
4. Encrypt persisted artifacts.

## Troubleshooting

### "Run not found"
- Check that `PEOPLEDD_OUTPUT_DIR` is mounted or persisted.
- Verify run_id is correct.

### "Analysis failed: OPENAI_API_KEY not set"
- Set `OPENAI_API_KEY` environment variable before starting API.
- Verify on Railway via Service → Variables.

### "Status: running" indefinitely
- Check Railway logs for errors.
- Increase memory limit if needed.
- Verify external API keys (Harvest, Exa, etc.).

### Slow analyses
- Consider "standard" analysis_depth instead of "deep".
- Disable features (`use_harvest=false`, `use_apify=false`) if not needed.
- Scale Railway service (more dynos, more CPU).

## API Stability

This API is **v0.1.0** and subject to change. Endpoint structure and response schemas are stable, but additional fields or optional parameters may be added.

---

For CLI usage and offline tools (calibration, dry-run), see the main [README.md](README.md).
