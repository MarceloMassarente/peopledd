# Deployment Guide for Railway

Complete guide to deploy peopledd REST API on Railway with persistent storage and external tool integration.

## Prerequisites

- GitHub account with repository
- Railway account (free tier available)
- API keys for external services (OPENAI_API_KEY, etc.)

## Step 1: Connect Repository to Railway

1. Go to [railway.app](https://railway.app)
2. Log in with GitHub
3. Create new project → "Deploy from GitHub repo"
4. Select your peopledd repository
5. Railway auto-detects the Dockerfile

## Step 2: Configure Environment Variables

In Railway dashboard, go to **Service → Variables** and add:

```
# === API Server ===
PORT=8000
HOST=0.0.0.0
PEOPLEDD_OUTPUT_DIR=/app/runs

# === Required API Keys ===
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
OPENAI_MODEL_MINI=gpt-4o-mini

# === Optional: Additional Services ===
EXA_API_KEY=...
HARVEST_API_KEY=...
BROWSERLESS_ENDPOINT=...
BROWSERLESS_TOKEN=...
JINA_API_KEY=...
SEARXNG_URL=...
PERPLEXITY_API_KEY=...
```

**Tip**: Mark sensitive vars as "Protected" in Railway dashboard.

## Step 3: Add Persistent Volume for Artifacts

1. In Railway dashboard → **Service → Storage**
2. Click **Add** → Create new volume
3. **Mount path**: `/app/runs`
4. **Size**: Start with 1GB (auto-expands)

This ensures run artifacts persist across deployments.

## Step 4: Deploy

Click **Deploy** in Railway dashboard. The build takes ~3-5 minutes.

After deployment:
- Railway provides a public URL: `https://<service-name>.<project>.railway.app`
- Test health: `curl https://<service-name>.<project>.railway.app/health`

## Step 5: Access API

Your API is now live at:

```
https://<service-name>.<project>.railway.app
```

### Example API Call

```bash
curl -X POST https://<service-name>.<project>.railway.app/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "company_name": "Itaú Unibanco",
    "country": "BR",
    "company_type_hint": "listed"
  }'
```

## Integration with External Tools

### OpenAPI Discovery

Any tool can fetch the API schema:

```bash
curl https://<service-name>.<project>.railway.app/openapi.json
```

Use this to auto-generate clients or integrate with Zapier, Make, n8n, etc.

### Example: Polling for Results (Python)

```python
import requests
import time

API_URL = "https://<service-name>.<project>.railway.app"

# Start analysis
response = requests.post(f"{API_URL}/analyze", json={
    "company_name": "Itaú Unibanco",
    "country": "BR"
})
run_id = response.json()["run_id"]

# Poll for completion
while True:
    status = requests.get(f"{API_URL}/runs/{run_id}/status").json()
    if status["status"] in ("completed", "error"):
        break
    time.sleep(10)

# Get results
result = requests.get(f"{API_URL}/runs/{run_id}/result").json()
brief = requests.get(f"{API_URL}/runs/{run_id}/brief").json()
```

### Example: n8n Workflow Integration

1. Create n8n workflow
2. Use **HTTP Request** node:
   - **URL**: `https://<service-name>.<project>.railway.app/analyze`
   - **Method**: POST
   - **Body**: Company name, type, etc.
3. Use **Poll** to wait for completion
4. Use second HTTP request to fetch `/runs/{run_id}/result`

## Monitoring

### Logs

Railway dashboard → **Service → Logs** shows:
- Startup logs
- API requests
- Pipeline execution
- Errors

### Health Checks

Railway automatically pings `/health` every 30 seconds (configured in Dockerfile).

Status page shows:
- ✓ **Healthy**: API is running
- ✗ **Unhealthy**: Check logs for errors

## Scaling

### High Concurrency

For many simultaneous analyses:

1. **Railway Plan**: Upgrade to Pro (better resource limits)
2. **Replicas**: Add multiple instances (load balanced automatically)
3. **Memory**: Increase RAM allocation per instance
4. **Storage**: Expand volume size

### Recommended Configuration for Production

- **Replicas**: 2-3 (for redundancy)
- **Memory**: 2GB minimum per replica
- **Volume**: 10GB minimum
- **CPU**: Standard (1x)

## Cost Estimation

Railway pricing (as of 2025):

| Component | Cost |
|-----------|------|
| Compute (1 instance, 512MB) | ~$5/month |
| Storage (1GB volume) | ~$1/month |
| Outbound bandwidth | ~$0.10 per GB |
| **Total (small)** | **~$6–10/month** |

Scaling up (2 replicas, 2GB each, 10GB storage): ~$30–50/month.

## Troubleshooting

### "502 Bad Gateway" Error

**Cause**: API container crashed or failed to start.

**Solution**:
1. Check Railway logs → **Service → Logs**
2. Verify all required env vars are set (especially OPENAI_API_KEY)
3. Restart service: Dashboard → **Restart**

### Analyses Timeout

**Cause**: Pipeline exceeds HTTP timeout (Railway default: ~30min).

**Solution**:
- Use async polling (don't wait for HTTP response)
- Check `PEOPLEDD_OUTPUT_DIR` volume is mounted
- Monitor Railway logs for pipeline errors

### "Run artifact not found"

**Cause**: Volume not mounted or corrupted.

**Solution**:
1. Verify volume mounted at `/app/runs`
2. Restart service
3. Re-run analysis

### Out of Memory

**Cause**: Replica memory limit exceeded during deep analysis.

**Solution**:
1. Increase memory per instance (Railway dashboard)
2. Use "standard" analysis_depth instead of "deep"
3. Reduce concurrency or add more replicas

## Custom Domain

To use your own domain:

1. Railway → **Service → Domain**
2. Click **Add Domain**
3. Point your DNS to Railway (instructions provided)

Example: `api.yourdomain.com/analyze`

## Backup & Disaster Recovery

### Automated Backups (Railway)

Railway can snapshot your volume:

1. Dashboard → **Storage → Backups**
2. Enable automatic snapshots

### Manual Export

To download artifacts locally:

```bash
# Via SSH (if enabled)
railway shell
tar -czf /tmp/runs-backup.tar.gz /app/runs/
```

Or use Railway File Browser to download directly.

## Next Steps

1. **Test locally** with `docker-compose up` before deploying
2. **Monitor logs** for first 24 hours
3. **Run calibration** on sample analyses: `python -m peopledd.tools.calibrate --runs-dir /app/runs`
4. **Implement auth** if exposing to untrusted clients (add middleware)

## Support

- **Railway Docs**: [https://docs.railway.app](https://docs.railway.app)
- **peopledd API Docs**: See [API.md](API.md)
- **GitHub Issues**: peopledd repository
