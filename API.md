# peopledd REST API

A REST API wrapper for the company governance X-ray pipeline. Exposes HTTP endpoints for external tools and services to trigger analyses, monitor progress, and retrieve results.

## Quick Start

### Local Development

```bash
# Install with API dependencies
pip install -e ".[strategy]"

# Run the API server
python -m peopledd.api
```

Server starts at `http://localhost:8000`

### Railway Deployment

1. **Create a Railway project** and link your GitHub repository.

2. **Set environment variables** in Railway console:
   ```
   OPENAI_API_KEY=sk-...
   EXA_API_KEY=...
   HARVEST_API_KEY=...
   # ... other API keys as needed
   ```

3. **Enable volumes** for persistent run artifacts:
   - Mount `/app/runs` to a Railway volume (recommended for production)

4. **Deploy**: Railway will auto-build from Dockerfile and start the service.

Access: `https://<your-railway-domain>/`

## Endpoints

### 🏥 Health Check

**GET** `/health`

Simple health check for orchestration/monitoring.

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "timestamp": "2025-01-15T10:30:00.000000"
}
```

---

### 🚀 Start Analysis

**POST** `/analyze`

Trigger a new analysis asynchronously. Returns immediately with a `run_id`.

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

**Response (202 Accepted):**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "message": "Analysis queued for Itaú Unibanco",
  "started_at": "2025-01-15T10:30:00.000000"
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

### 📊 Check Run Status

**GET** `/runs/{run_id}/status`

Poll the status of a queued or running analysis.

**Response:**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "completed_at": null,
  "error": null
}
```

**Statuses:**
- `queued` — Waiting in queue
- `running` — Currently executing
- `completed` — Finished successfully
- `error` — Failed

**Example polling loop (Python):**
```python
import requests
import time

run_id = "550e8400-e29b-41d4-a716-446655440000"
base_url = "http://localhost:8000"

while True:
    response = requests.get(f"{base_url}/runs/{run_id}/status")
    status = response.json()
    print(f"Status: {status['status']}")
    
    if status['status'] in ('completed', 'error'):
        break
    
    time.sleep(5)  # Poll every 5 seconds
```

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

**Response:**
```json
{
  "runs": [
    { "run_id": "550e8400-e29b-41d4-a716-446655440000" },
    { "run_id": "550e8400-e29b-41d4-a716-446655440001" }
  ],
  "count": 2
}
```

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
