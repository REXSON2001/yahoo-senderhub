FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    --no-install-recommends

# Install specific working Chrome version (119.0.6045.105)
RUN wget -q -O /tmp/chrome.deb "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb" \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb

# Use webdriver-manager to handle ChromeDriver automatically
RUN pip install webdriver-manager

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p screenshots data logs

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome
ENV DISPLAY=:99
ENV PYTHONPATH=/app

# Run the scraper
CMD ["python", "yahoo_persistent_enhanced_complete.py"]