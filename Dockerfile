FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh kode backend
COPY . .

# Jalankan Uvicorn pada port 8080 (standar internal Fly.io)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
