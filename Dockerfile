FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash teleops

WORKDIR /app

COPY requirements ./requirements

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    -r requirements/base.txt

COPY . .

RUN mkdir -p \
    /app/data/logs \
    /app/data/uploads \
    /app/data/cache \
    /app/data/backups

RUN chown -R teleops:teleops /app

USER teleops

CMD ["python", "main.py"]
