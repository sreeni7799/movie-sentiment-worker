FROM python:3.12-slim-bookworm

# Create non-root user
RUN groupadd -r worker && useradd -r -g worker worker

# Set working directory
WORKDIR /app

# Update system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy shared database code first
COPY shared/ ./shared/

# Copy worker service code
COPY worker_app.py .
COPY worker_tasks.py .

# Change ownership to non-root user
RUN chown -R worker:worker /app

# Switch to non-root user
USER worker

# Run the worker
CMD ["python", "worker_app.py"]