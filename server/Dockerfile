# syntax=docker/dockerfile:1

FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY core/ core/
COPY templates/ templates/
COPY static/ ./static/

EXPOSE 5000

ENTRYPOINT ["python", "-u", "app.py"]
