FROM python:3.11-slim

# -----------------------------------
# Install system dependencies
# -----------------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------
# Create non-root user (security)
# -----------------------------------
RUN useradd -m apiuser

# -----------------------------------
# Set working directory
# -----------------------------------
WORKDIR /app

# -----------------------------------
# Install Python dependencies
# -----------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------
# Copy application code
# -----------------------------------
COPY . .

# -----------------------------------
# Create required directories
# -----------------------------------
RUN mkdir -p screenshots data \
    && chown -R apiuser:apiuser /app

# -----------------------------------
# Environment variables
# -----------------------------------
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# -----------------------------------
# Switch to non-root user
# -----------------------------------
USER apiuser

# -----------------------------------
# Expose API port
# -----------------------------------
EXPOSE 8001

# -----------------------------------
# Run FastAPI with Uvicorn (PRODUCTION)
# -----------------------------------
CMD ["uvicorn", "api_server_enhanced:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "2"]
