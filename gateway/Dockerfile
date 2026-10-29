# =============================================================================
# Stage 1 — build: compile grpcio + install all deps into /install
# =============================================================================
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# grpcio needs gcc/g++ for compilation on slim images
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .

RUN pip install --upgrade pip --no-cache-dir \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# =============================================================================
# Stage 2 — runtime
# =============================================================================
FROM python:3.12-slim AS runtime

LABEL maintainer="gateway-service" \
      description="S3-compatible API gateway for distributed storage" \
      version="1.0.0"

RUN addgroup --system appgroup \
 && adduser  --system --ingroup appgroup --no-create-home appuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/install/lib/python3.12/site-packages \
    # Application defaults (overridden in docker-compose)
    ETCD_HOST=etcd \
    ETCD_PORT=2379 \
    CHUNKING_MODE=fixed \
    CHUNK_SIZE=4194304 \
    REPLICATION_FACTOR=3 \
    WRITE_QUORUM=2 \
    READ_QUORUM=2 \
    FAILURE_THRESHOLD=15 \
    FAILURE_CHECK_INTERVAL=5 \
    REPAIR_INTERVAL=30 \
    DEFAULT_ERASURE_K=2 \
    DEFAULT_ERASURE_M=2 \
    GATEWAY_PORT=9000 \
    STORAGE_NODES=node-1=http://storage_node1:8000,node-2=http://storage_node2:8000,node-3=http://storage_node3:8000,node-4=http://storage_node4:8000

WORKDIR /app

COPY --from=builder /install /install
COPY gateway.py chunker.py ring.py metadata.py quorum.py erasure.py failure_detector.py repair_worker.py ./

RUN chown -R appuser:appgroup /app
USER appuser

EXPOSE 9000

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c \
        "import urllib.request, sys; \
         r = urllib.request.urlopen('http://localhost:9000/health', timeout=4); \
         sys.exit(0 if r.status == 200 else 1)"

CMD ["python", "-m", "uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "1", "--log-level", "info"]
