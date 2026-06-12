# syntax=docker/dockerfile:1

FROM python:3.11-alpine

WORKDIR /app

COPY central_aggregator.py .

RUN adduser -D -u 1000 appuser \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-u", "central_aggregator.py"]
