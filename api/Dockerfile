FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p screenshots data

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8001

# Run the API server
CMD ["python", "api_server_enhanced.py"]