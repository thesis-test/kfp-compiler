FROM --platform=linux/amd64 python:3.11-slim

# Non-root user matching the securityContext in the WorkflowTemplates
RUN groupadd --gid 1000 appgroup \
    && useradd  --uid 1000 --gid appgroup --no-create-home appuser

# ── System dependencies ─────────────────────────────────────────────────────
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── ORAS CLI (OCI Registry As Storage — CNCF standard) ─────────────────────
# ORAS speaks the OCI Distribution Spec natively: works with Harbor, Nexus,
# ECR, ACR, GCR — any compliant registry.  kfp.registry.RegistryClient only
# targets Google Artifact Registry and cannot be used here.
ARG ORAS_VERSION=1.2.2
# SHA256 from https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_checksums.txt
ARG ORAS_SHA256=bff970346470e5ef888e9f2c0bf7f8ee47283f5a45207d6e7a037da1fb0eae0d
RUN curl -sSL \
    "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" \
    -o /tmp/oras.tar.gz \
    && printf '%s  /tmp/oras.tar.gz\n' "${ORAS_SHA256}" | sha256sum --check --strict \
    && tar -xz -C /usr/local/bin -f /tmp/oras.tar.gz oras \
    && rm /tmp/oras.tar.gz \
    && oras version

# ── Python dependencies ─────────────────────────────────────────────────────
# kfp ships the `kfp dsl compile` CLI and kfp.client.  No boto3 needed —
# the OCI registry (via ORAS) is the single artifact store.
RUN pip install --no-cache-dir "kfp==2.16.0"

# ── Application ─────────────────────────────────────────────────────────────
WORKDIR /opt/kfp-compiler
COPY deploy_pipeline.py /opt/kfp-compiler/deploy_pipeline.py

USER appuser

ENTRYPOINT ["python", "/opt/kfp-compiler/deploy_pipeline.py"]
