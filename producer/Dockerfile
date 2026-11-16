# Replay producer image. Built from the repo root (see docker-compose.yml) so it can
# include docs/schemas for the test consumer.
FROM python:3.12.12-slim-bookworm

# Don't write .pyc files; print logs immediately instead of buffering them.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    SCHEMA_DIR=/app/schemas \
    KAFKA_BOOTSTRAP=kafka:9092

WORKDIR /app

# Install dependencies first: this layer is cached until requirements.txt changes.
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docs/schemas/ /app/schemas/
COPY producer/ /app/

# Run as a non-root user.
RUN useradd --create-home --uid 1000 matchpulse
USER matchpulse

ENTRYPOINT ["python", "replay.py"]
CMD ["--help"]
