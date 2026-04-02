FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ ./src/

# Data directory is mounted at runtime via docker-compose volume
# so we only create the mount point here
RUN mkdir -p /app/data/input

CMD ["python", "src/etl.py"]
