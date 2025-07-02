FROM python:3.12-slim-bookworm

RUN groupadd -r worker && useradd -r -g worker worker

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/

COPY worker_app.py .
COPY worker_tasks.py .

RUN chown -R worker:worker /app

USER worker

CMD ["python", "worker_app.py"]