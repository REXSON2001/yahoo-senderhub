FROM python:3.11-slim

# -------------------------------
# Install system dependencies
# -------------------------------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libasound2 \
    libxshmfence1 \
    libu2f-udev \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Install Google Chrome (stable)
# -------------------------------
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# -------------------------------
# Install matching ChromeDriver
# -------------------------------
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d'.' -f1) && \
    wget -q "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION}" -O /tmp/LATEST && \
    wget -q "https://chromedriver.storage.googleapis.com/$(cat /tmp/LATEST)/chromedriver_linux64.zip" -O /tmp/chromedriver.zip && \
    unzip /tmp/chromedriver.zip -d /usr/bin/ && \
    chmod +x /usr/bin/chromedriver && \
    rm /tmp/chromedriver.zip /tmp/LATEST

# -------------------------------
# Python setup
# -------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p screenshots data logs

# -------------------------------
# Environment variables
# -------------------------------
ENV CHROME_BIN=/usr/bin/google-chrome
ENV PYTHONPATH=/app

# -------------------------------
# Run scraper
# -------------------------------
CMD ["python", "yahoo_persistent_enhanced_complete.py"]
