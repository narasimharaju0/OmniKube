# syntax=docker/dockerfile:1

FROM python:3.11-alpine

WORKDIR /app

COPY app.py .

EXPOSE 5000

ENTRYPOINT ["python", "-u", "app.py"]
