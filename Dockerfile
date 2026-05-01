# Slim Python base — small image, official, security-patched
FROM python:3.12-slim

# Don't write .pyc files; flush stdout/stderr immediately so logs show in Cloud Run
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first (separate layer = Docker caches this when only app code changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY app.py ./
COPY static/ ./static/

# Cloud Run injects the PORT env var (default 8080). app.py already reads it.
ENV PORT=8080
EXPOSE 8080

CMD ["python", "app.py"]
