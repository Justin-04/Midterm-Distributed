FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM python:3.12-slim AS runtime
LABEL description="Distributed Storage API Gateway"
RUN addgroup --system app && adduser --system --ingroup app --no-create-home appuser
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/install/lib/python3.12/site-packages \
    ETCD_HOST=etcd \
    ETCD_PORT=2379 \
    GATEWAY_PORT=9100 \
    JWT_SECRET_KEY=change-me-in-production \
    STORAGE_NODES=node-1=http://storage_node1:8000,node-2=http://storage_node2:8000,node-3=http://storage_node3:8000,node-4=http://storage_node4:8000
WORKDIR /app
COPY --from=builder /install /install
COPY . .
RUN chown -R appuser:app /app
USER appuser
EXPOSE 9100
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://localhost:9100/health',timeout=4); sys.exit(0 if r.status==200 else 1)"
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9100", "--workers", "1", "--log-level", "info"]
