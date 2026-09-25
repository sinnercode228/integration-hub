# syntax=docker/dockerfile:1.7

# 1) Dashboard (served by the API at /admin/, talking to the live admin API)
FROM node:22-alpine AS dashboard
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build:admin

# 2) Python wheel
FROM python:3.13-slim AS build
WORKDIR /src
COPY backend/pyproject.toml backend/README.md ./
COPY backend/src ./src
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# 3) Runtime
FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RELAY_CONFIG_PATH=/app/config/relay.yaml \
    RELAY_SQLITE_PATH=/app/data/relay.db \
    RELAY_DASHBOARD_DIR=/app/dashboard
WORKDIR /app
RUN useradd --create-home --uid 10001 relay
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
COPY config ./config
COPY --from=dashboard /app/dist ./dashboard
RUN mkdir -p /app/data && chown relay:relay /app/data
USER relay
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz').status == 200 else 1)"
CMD ["relay", "serve", "--host", "0.0.0.0", "--port", "8000"]
