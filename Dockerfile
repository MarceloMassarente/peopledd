# Multi-stage build for peopledd API
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY peopledd/ /app/

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[strategy]"

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

# Create directory for run artifacts (can be mounted as volume)
RUN mkdir -p /app/runs

# Set Python to be unbuffered
ENV PYTHONUNBUFFERED=1
ENV PEOPLEDD_OUTPUT_DIR=/app/runs
ENV PORT=8000
ENV HOST=0.0.0.0

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Run the API server
CMD ["python", "-m", "peopledd.api"]
