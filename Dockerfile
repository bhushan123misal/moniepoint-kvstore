FROM python:3.11.5

RUN useradd -m kvuser
WORKDIR /app

COPY kvstore /app/kvstore
COPY main.py /app/main.py

RUN mkdir -p /data && chown -R kvuser:kvuser /data /app
USER kvuser

EXPOSE 7070

ENV DATA_DIR=/data \
    PORT=7070 \
    SNAPSHOT_INTERVAL_S=60 \
    WAL_MAX_BYTES=134217728 \
    MEMTABLE_MB=64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "main.py", "--data-dir", "/data", "--port", "7070"]
