
FROM python:3.10-slim

# 1) Install CA certificates and system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 2) Set workdir, copy requirements first for better Docker layer caching
WORKDIR /app
COPY requirements.txt .

# 3) Upgrade pip and install deps; add trusted hosts for TLS edge cases
#    (trusted-host is a workaround; prefer Fix B with proper CA if on corp proxy)
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
        --trusted-host pypi.org \
        --trusted-host files.pythonhosted.org \
        -r requirements.txt

# 4) Copy the app code and run uvicorn
COPY . .
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
