# syntax=docker/dockerfile:1

FROM python:3.11-alpine AS builder

RUN apk add --no-cache gcc musl-dev linux-headers python3-dev

WORKDIR /build
RUN pip install --no-cache-dir --prefix=/install psutil


FROM python:3.11-alpine AS runtime

WORKDIR /app

COPY --from=builder /install /usr/local
COPY metrics_collector.py .

RUN adduser -D -u 1000 appuser \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-u", "metrics_collector.py"]
