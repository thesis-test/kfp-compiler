# ---------------------------------------------------------------------------
# Stage 1: Toolchain Downloader
# Securely sources the ORAS binary from the official OCI image.
# ---------------------------------------------------------------------------
FROM ghcr.io/oras-project/oras:v1.2.2 AS oras-source

# ---------------------------------------------------------------------------
# Stage 2: MLOps CI Builder (Final Runtime)
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm

ENV KFP_VERSION="2.15.0"
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=oras-source /bin/oras /usr/local/bin/oras

RUN groupadd -g 1000 argo-ci && \
    useradd -u 1000 -g argo-ci -m -s /bin/bash argo-ci

RUN python -m venv ${VIRTUAL_ENV} && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir kfp==${KFP_VERSION} PyYAML==6.0.1 && \
    chown -R argo-ci:argo-ci ${VIRTUAL_ENV}

COPY ci_orchestrator.py /usr/local/bin/ci_orchestrator.py
RUN chmod +x /usr/local/bin/ci_orchestrator.py && \
    chown argo-ci:argo-ci /usr/local/bin/ci_orchestrator.py

USER 1000
WORKDIR /workspace

ENTRYPOINT ["python3", "/usr/local/bin/ci_orchestrator.py"]