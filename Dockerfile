# ---------------------------------------------------------------------------
# Stage 1: Toolchain Downloader
# ---------------------------------------------------------------------------
FROM ghcr.io/oras-project/oras:v1.3.1 AS oras-source

# ---------------------------------------------------------------------------
# Stage 2: Python Builder (Dependency Isolation)
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

ENV VIRTUAL_ENV="/opt/venv"
RUN python -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Install dependencies and pre-compile Python bytecode for faster cold starts
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir kfp==2.16.0 PyYAML==6.0.1 && \
    python -m compileall ${VIRTUAL_ENV}

# ---------------------------------------------------------------------------
# Stage 3: Final Runtime
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm

ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
# Force Python to flush logs immediately to Argo
ENV PYTHONUNBUFFERED=1 

# Install ONLY essential runtime OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy ORAS binary
COPY --from=oras-source /bin/oras /usr/local/bin/oras

# Copy pre-compiled Python environment from builder (Owned by root, Read/Execute for all)
COPY --from=builder /opt/venv /opt/venv

# Setup unprivileged user
RUN groupadd -g 1000 argo-ci && \
    useradd -u 1000 -g argo-ci -m -s /bin/bash argo-ci

# Copy and pre-compile the execution script
COPY ci_orchestrator.py /app/ci_orchestrator.py
RUN python -m py_compile /app/ci_orchestrator.py

USER 1000
WORKDIR /workspace

ENTRYPOINT ["python", "/app/ci_orchestrator.py"]