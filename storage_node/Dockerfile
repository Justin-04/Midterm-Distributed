# =============================================================================
# Stage 1 — dependency builder
# =============================================================================
# Use the full slim image to compile any C-extension wheels that might be
# needed (e.g. uvloop inside uvicorn[standard]).  The result is then copied
# into the final runtime layer, keeping the final image lean.
# =============================================================================
FROM python:3.12-slim AS builder

# Prevent .pyc files and enable unbuffered stdout/stderr during the build.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Copy only the dependency manifest first so Docker can cache this layer
# independently of application code changes.
COPY requirements.txt .

# Install into an isolated prefix so we can COPY just that tree later.
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# =============================================================================
# Stage 2 — runtime image
# =============================================================================
FROM python:3.12-slim AS runtime

LABEL maintainer="storage-node-service" \
      description="Content-addressed storage node (FastAPI)" \
      version="1.0.0"

# Security best-practice: run as a non-root user.
RUN addgroup --system appgroup \
 && adduser  --system --ingroup appgroup --no-create-home appuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Put the installed packages on PYTHONPATH.
    PYTHONPATH=/install/lib/python3.12/site-packages \
    # Application defaults (overridden per-container in docker-compose).
    STORAGE_DIR=/data/chunks \
    NODE_ID=node-unknown \
    METADATA_SERVICE_URL=http://metadata:8001 \
    HEARTBEAT_INTERVAL=5 \
    GATEWAY_URL=http://gateway:9000 \
    SCRUB_INTERVAL=60 \
    PORT=8000

WORKDIR /app

# Pull in the pre-built packages from the builder stage.
COPY --from=builder /install /install

# Copy application source.
COPY main.py storage.py heartbeat.py scrubber.py ./

# Create the default storage directory and hand ownership to the app user.
RUN mkdir -p /data/chunks \
 && chown -R appuser:appgroup /app /data

USER appuser

# Expose the service port.
EXPOSE 8000

# Healthcheck so orchestrators know when the node is ready.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c \
        "import urllib.request, sys; \
         r = urllib.request.urlopen('http://localhost:8000/health', timeout=4); \
         sys.exit(0 if r.status == 200 else 1)"

# Launch via uvicorn.  Workers=1 keeps the asyncio event-loop single-threaded,
# which is correct for I/O-bound chunk serving.  Scale horizontally via
# multiple containers instead.
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "info"]
