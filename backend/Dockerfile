FROM python:3.11-slim

# Dependencias del sistema para OpenCV (lo tuyo) + Playwright/Chromium (nuevo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    wget \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    libjpeg-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Browser de Playwright (~150 MB, solo Chromium)
RUN playwright install chromium

# Copiar el resto del código (el volume del compose lo override en dev)
COPY . .

# Comando para iniciar FastAPI (igual que tenías)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]