# GameInsight clip desk – runs the gtavi-claudecode scripts behind a small HTTP API (server/clipdesk.py).
# Build context is the repo root:  docker build -f server/Dockerfile .
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg fonts-liberation2 fontconfig ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
# The captions ask for "Arial Black"/"Arial" (Windows fonts). Map them to Liberation Sans (metric-compatible, bold via style).
COPY server/fonts.conf /etc/fonts/local.conf
RUN fc-cache -f

WORKDIR /app
COPY requirements.txt .
# PyJWT: MCP token check; PyYAML: campaigns.yml; curl_cffi: browser TLS fingerprint for Kick (Cloudflare)
RUN pip install --no-cache-dir -r requirements.txt "PyJWT[crypto]>=2.9" "PyYAML>=6" "curl_cffi>=0.7"

COPY scripts/ scripts/
COPY skills/ skills/
COPY server/clipdesk.py server/campaigns.yml server/

ENV CLIP_DESK_HOME=/data \
    CLIPDESK_APP=/app \
    HF_HOME=/data/models \
    PYTHONUNBUFFERED=1 \
    PORT=8088

RUN useradd -m -u 1000 clipdesk && mkdir -p /data && chown clipdesk /data
USER clipdesk
VOLUME /data
EXPOSE 8088
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s CMD curl -fsS http://127.0.0.1:8088/health || exit 1
CMD ["python", "/app/server/clipdesk.py"]
