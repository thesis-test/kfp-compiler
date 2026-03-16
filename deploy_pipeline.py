"""
deploy_pipeline.py — Build-Once, Promote-Everywhere KFP v2 CI/CD.

Two subcommands with completely separate logic:

  build   (dev cluster — triggered by GitHub push)
    1. git diff  → find changed pipeline folders
    2. kfp dsl compile  → IR YAML
    3. oras push  → Harbor dev registry (tag = commit SHA)
    4. kfp upload → local KFP API  (import compiled YAML directly)

  promote (staging/prod cluster — triggered by Harbor replication webhook)
    1. oras pull  → download the artifact that just landed in local Harbor
    2. kfp upload → local KFP API
    No git, no polling, no compilation.

OCI artifact naming convention
  <OCI_REGISTRY>/<OCI_REPOSITORY>/<pipeline-folder-name>:<commit-sha>
  e.g.  harbor.dev.internal/ml-fraud-detection/fraud-model:abc1234ef

Why ORAS instead of kfp.registry.RegistryClient:
  kfp.RegistryClient targets Google Artifact Registry only — its REST API
  (/packages, /versions) does not exist on Harbor or any other OCI registry.
  ORAS (OCI Registry As Storage, CNCF project) speaks the OCI Distribution
  Spec and works with any compliant registry: Harbor, Nexus, ECR, ACR, GCR.

Why event-driven promotion instead of polling:
  Harbor fires a PUSH_ARTIFACT webhook when replication finishes.
  Argo Events in the target cluster receives it and triggers this script.
  Zero wasted container time, zero timeout tuning, instant response.

Exit codes:
  0  success (or nothing to do)
  1  compile / upload / ORAS error
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import subprocess
import sys
import tempfile
import urllib.parse

from kfp.client import Client as KfpClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("deploy-pipeline")


# ---------------------------------------------------------------------------
# Helpers — Git
# ---------------------------------------------------------------------------

def clone_repo(clone_url: str, token: str, dest: pathlib.Path, branch: str) -> None:
    """Shallow-clone `branch` of `clone_url` into `dest`.

    The PAT is embedded in the URL for git credential passing but is
    never logged — safe_cmd is passed to _run with the plain URL instead.
    """
    auth_url = clone_url.replace("https://", f"https://x-access-token:{token}@")
    base_cmd = ["git", "clone", "--quiet", "--depth", "50", "--branch", branch]
    _run(
        base_cmd + [auth_url, str(dest)],
        safe_cmd=base_cmd + [clone_url, str(dest)],  # token-free version for logs
    )


def fetch_sha(repo: pathlib.Path, sha: str) -> None:
    # Non-fatal: the SHA may already be present after shallow clone.
    subprocess.run(
        ["git", "-C", str(repo), "fetch", "--quiet", "--depth", "50", "origin", sha],
        check=False,
    )


def changed_pipeline_folders(repo: pathlib.Path, before: str, after: str) -> list[str]:
    """Return sorted unique top-level folder names under pipelines/ that changed."""
    result = subprocess.run(
        ["git", "-C", str(repo), "diff", "--name-only", before, after, "--", "pipelines/"],
        capture_output=True, text=True, check=True,
    )
    folders: set[str] = set()
    for line in result.stdout.strip().splitlines():
        parts = line.split("/")
        if len(parts) >= 2 and parts[0] == "pipelines" and parts[1]:
            folders.add(parts[1])
    return sorted(folders)


# ---------------------------------------------------------------------------
# Helpers — Compilation
# ---------------------------------------------------------------------------

def compile_pipeline(pipeline_dir: pathlib.Path, output_path: pathlib.Path) -> None:
    """Compile via `kfp dsl compile` CLI.

    Using the CLI (not importlib) is the only supported, stable interface.
    KFP v2 @dsl.component embeds packages_to_install into the IR YAML as
    metadata — no runtime pip install is needed.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "kfp", "dsl", "compile",
        "--py",     str(pipeline_dir / "main.py"),
        "--output", str(output_path),
    ]
    log.info("  Compile: %s", " ".join(cmd))
    _run(cmd, cwd=str(pipeline_dir))
    log.info("  Compiled  → %s  (%d bytes)", output_path.name, output_path.stat().st_size)


# ---------------------------------------------------------------------------
# Helpers — ORAS (OCI Registry As Storage — CNCF standard)
# ---------------------------------------------------------------------------
#
# ORAS pushes/pulls arbitrary files as OCI artifacts.  It speaks the OCI
# Distribution Spec natively and works with any compliant registry.
#
# Artifact media type:
#   application/vnd.kubeflow.pipeline.v2+yaml
# This type is stored in the OCI manifest's artifactType field and survives
# replication unchanged, so staging/prod can filter on it if desired.

_OCI_ARTIFACT_TYPE = "application/vnd.kubeflow.pipeline.v2+yaml"
_OCI_LAYER_TYPE    = "application/yaml"


def _oras_ref(registry: str, repository: str, name: str, tag: str) -> str:
    return f"{registry}/{repository}/{name}:{tag}"


def oras_push(
    registry: str,
    repository: str,
    pipeline_name: str,
    commit_sha: str,
    pipeline_yaml: pathlib.Path,
    username: str,
    password: str,
) -> pathlib.Path:
    """Push a compiled pipeline YAML as an OCI artifact.

    The file pushed to ORAS must be named <pipeline_name>.yaml so the
    artifact layer filename is deterministic and matches what oras_pull
    will download.

    Returns the stable (renamed) path so the caller can pass it directly
    to upload_to_kfp without reconstructing the name independently.
    """
    ref  = _oras_ref(registry, repository, pipeline_name, commit_sha)
    named = pipeline_yaml.parent / f"{pipeline_name}.yaml"
    if pipeline_yaml != named:
        pipeline_yaml.rename(named)

    cmd = [
        "oras", "push", ref,
        f"{named}:{_OCI_LAYER_TYPE}",
        "--artifact-type", _OCI_ARTIFACT_TYPE,
        "--username", username,
        "--password-stdin",   # avoids password in process args / ps output
    ]
    log.info("  ORAS push → %s", ref)
    _run(cmd, input_text=password)
    log.info("  Pushed    ✓")
    return named


def oras_pull(
    registry: str,
    repository: str,
    pipeline_name: str,
    commit_sha: str,
    dest_dir: pathlib.Path,
    username: str,
    password: str,
) -> pathlib.Path:
    """Pull a pipeline artifact from the local (already-replicated) registry.

    Returns the path to the downloaded YAML file.
    """
    ref   = _oras_ref(registry, repository, pipeline_name, commit_sha)
    dest_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "oras", "pull", ref,
        "--output", str(dest_dir),
        "--username", username,
        "--password-stdin",
    ]
    log.info("  ORAS pull ← %s", ref)
    _run(cmd, input_text=password)

    # oras preserves layer filenames; we pushed it as <pipeline_name>.yaml
    yaml_path = dest_dir / f"{pipeline_name}.yaml"
    if not yaml_path.exists():
        # Fallback: any .yaml in the output directory
        candidates = list(dest_dir.glob("*.yaml"))
        if not candidates:
            raise FileNotFoundError(f"oras pull produced no .yaml in {dest_dir}")
        yaml_path = candidates[0]

    log.info("  Pulled    → %s  (%d bytes)", yaml_path.name, yaml_path.stat().st_size)
    return yaml_path


# ---------------------------------------------------------------------------
# Helpers — KFP API
# ---------------------------------------------------------------------------

def make_kfp_client(kfp_host: str, namespace: str, sa_token: str | None) -> KfpClient:
    return KfpClient(host=kfp_host, existing_token=sa_token, namespace=namespace)


def find_pipeline_id(client: KfpClient, pipeline_name: str, namespace: str) -> str | None:
    """Exact-match server-side filter — avoids full scan across pages."""
    f = json.dumps({"predicates": [{
        "operation": "EQUALS", "key": "display_name", "string_value": pipeline_name,
    }]})
    result = client.list_pipelines(namespace=namespace, filter=urllib.parse.quote(f))
    return next(
        (p.pipeline_id for p in (result.pipelines or []) if p.display_name == pipeline_name),
        None,
    )


def _version_exists(client: KfpClient, pipeline_id: str, version_name: str) -> bool:
    """Return True if a pipeline version with this name already exists.

    Used for idempotency: a retried workflow step must not fail because
    the first (partially successful) attempt already created the version.
    """
    f = json.dumps({"predicates": [{
        "operation": "EQUALS", "key": "display_name", "string_value": version_name,
    }]})
    result = client.list_pipeline_versions(
        pipeline_id=pipeline_id, filter=urllib.parse.quote(f)
    )
    return any(
        v.display_name == version_name
        for v in (result.pipeline_versions or [])
    )


def upload_to_kfp(
    client: KfpClient,
    pipeline_path: pathlib.Path,
    pipeline_name: str,
    version_name: str,
    commit_sha: str,
    namespace: str,
) -> None:
    pipeline_id = find_pipeline_id(client, pipeline_name, namespace)
    if pipeline_id is None:
        log.info("  KFP: creating new pipeline '%s'", pipeline_name)
        p = client.upload_pipeline(
            pipeline_package_path=str(pipeline_path),
            pipeline_name=pipeline_name,
            namespace=namespace,
        )
        log.info("  KFP: created pipeline_id=%s", p.pipeline_id)
    else:
        # Idempotency guard: if this step is retried after a partial success
        # (e.g. upload succeeded but the workflow pod crashed before reporting),
        # the version already exists — treat that as success, not an error.
        if _version_exists(client, pipeline_id, version_name):
            log.info(
                "  KFP: version '%s' already exists on pipeline_id=%s — skipping (idempotent)",
                version_name, pipeline_id,
            )
            return
        log.info("  KFP: uploading version '%s' to pipeline_id=%s", version_name, pipeline_id)
        client.upload_pipeline_version(
            pipeline_package_path=str(pipeline_path),
            pipeline_version_name=version_name,
            pipeline_id=pipeline_id,
            description=f"commit {commit_sha}",
        )
    log.info("  KFP: upload complete ✓")


# ---------------------------------------------------------------------------
# Subprocess wrapper
# ---------------------------------------------------------------------------

def _run(
    cmd: list[str],
    cwd: str | None = None,
    input_text: str | None = None,
    safe_cmd: list[str] | None = None,
) -> None:
    """Run a subprocess, streaming stdout/stderr to the log.  Raises on failure.

    safe_cmd: an optional version of cmd with credentials redacted, used
    for log messages so tokens never appear in pod logs.
    """
    result = subprocess.run(
        cmd,
        cwd=cwd,
        input=input_text.encode() if input_text else None,
        capture_output=True,
    )
    if result.stdout:
        # Always log at INFO so subprocess output appears in workflow pod logs
        # regardless of the root log level configuration.
        log.info(result.stdout.decode(errors="replace").strip())
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        display = " ".join(safe_cmd if safe_cmd is not None else cmd)
        log.error("Command failed (exit %d): %s\n%s", result.returncode, display, stderr)
        raise subprocess.CalledProcessError(result.returncode, cmd, stderr)
    elif result.stderr:
        # Log stderr from successful invocations as warnings (e.g. git hints).
        log.warning(result.stderr.decode(errors="replace").strip())


# ---------------------------------------------------------------------------
# Shared credential / client helper
# ---------------------------------------------------------------------------

def _load_env() -> dict:
    """Read all credentials from env / mounted secrets."""
    oci_registry   = os.environ.get("OCI_REGISTRY", "").strip()
    oci_repository = os.environ.get("OCI_REPOSITORY", "").strip()
    oci_username   = os.environ.get("OCI_USERNAME",  "").strip()
    oci_password   = os.environ.get("OCI_PASSWORD",  "").strip()
    kfp_host       = os.environ.get("KFP_HOST",
                         "http://ml-pipeline.kubeflow.svc.cluster.local:8888")

    missing = [k for k, v in {
        "OCI_REGISTRY":   oci_registry,
        "OCI_REPOSITORY": oci_repository,
        "OCI_USERNAME":   oci_username,
        "OCI_PASSWORD":   oci_password,
    }.items() if not v]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    return dict(
        oci_registry=oci_registry,
        oci_repository=oci_repository,
        oci_username=oci_username,
        oci_password=oci_password,
        kfp_host=kfp_host,
    )


def _read_tokens(token_path: str, sa_token_path: str) -> tuple[str, str | None]:
    gh_token = pathlib.Path(token_path).read_text().strip()
    sa_file  = pathlib.Path(sa_token_path)
    sa_token = sa_file.read_text().strip() if sa_file.exists() else None
    return gh_token, sa_token


# ---------------------------------------------------------------------------
# Subcommand: build  (dev cluster)
# ---------------------------------------------------------------------------

def cmd_build(args: argparse.Namespace) -> None:
    """Compile changed pipelines and push to the dev OCI registry + KFP API."""
    env = _load_env()
    gh_token, sa_token = _read_tokens(args.token_path, args.sa_token_path)

    namespace  = args.namespace
    commit_sha = args.commit_sha
    # Normalize: 'refs/heads/dev' → 'dev'
    branch = args.branch.removeprefix("refs/heads/")

    log.info("=== BUILD  branch=%s  sha=%s  namespace=%s ===", branch, commit_sha, namespace)

    # Use a single TemporaryDirectory for the entire build so all temp files
    # (cloned repo, compiled YAMLs) are cleaned up automatically on exit.
    with tempfile.TemporaryDirectory() as tmpdir:
        root = pathlib.Path(tmpdir)
        repo_dir = root / "repo"

        log.info("Cloning %s ...", args.repo_url)
        clone_repo(args.repo_url, gh_token, repo_dir, branch)
        fetch_sha(repo_dir, args.before_sha)

        folders = changed_pipeline_folders(repo_dir, args.before_sha, commit_sha)
        if not folders:
            log.info("No pipeline folders changed — nothing to do.")
            return
        log.info("Changed: %s", ", ".join(folders))

        kfp_client = make_kfp_client(env["kfp_host"], namespace, sa_token)
        version_name = commit_sha[:7]
        errors: list[str] = []

        for name in folders:
            pipeline_dir = repo_dir / "pipelines" / name
            if not (pipeline_dir / "main.py").exists():
                log.warning("Skipping '%s' — no main.py", name)
                continue
            log.info("── %s ──────────────────────────────", name)
            try:
                compiled = root / "compiled" / f"{name}.yaml"
                compiled.parent.mkdir(parents=True, exist_ok=True)

                # 1. Compile IR YAML
                compile_pipeline(pipeline_dir, compiled)

                # 2. Push to dev OCI registry  (tag = full SHA for exact promotion match)
                # oras_push renames the file and returns the stable path.
                kfp_yaml = oras_push(
                    registry=env["oci_registry"],
                    repository=env["oci_repository"],
                    pipeline_name=name,
                    commit_sha=commit_sha,
                    pipeline_yaml=compiled,
                    username=env["oci_username"],
                    password=env["oci_password"],
                )
                # 3. Upload to dev KFP API
                upload_to_kfp(kfp_client, kfp_yaml, name, version_name, commit_sha, namespace)

            except Exception:
                log.exception("Failed: '%s'", name)
                errors.append(name)

    if errors:
        log.error("Build failed for: %s", ", ".join(errors))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand: promote  (staging / prod cluster)
# ---------------------------------------------------------------------------

def cmd_promote(args: argparse.Namespace) -> None:
    """Pull a replicated OCI artifact and upload it to the local KFP API.

    Triggered by the Harbor PUSH_ARTIFACT webhook (replication complete).
    No git clone, no compilation, no polling.
    """
    env = _load_env()
    sa_file = pathlib.Path(args.sa_token_path)
    sa_token = sa_file.read_text().strip() if sa_file.exists() else None

    pipeline_name = args.pipeline_name
    commit_sha    = args.commit_sha
    namespace     = args.namespace
    version_name  = commit_sha[:7]

    log.info(
        "=== PROMOTE  pipeline=%s  sha=%s  namespace=%s ===",
        pipeline_name, commit_sha, namespace,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        dest_dir = pathlib.Path(tmpdir) / pipeline_name
        pipeline_yaml = oras_pull(
            registry=env["oci_registry"],
            repository=env["oci_repository"],
            pipeline_name=pipeline_name,
            commit_sha=commit_sha,
            dest_dir=dest_dir,
            username=env["oci_username"],
            password=env["oci_password"],
        )
        kfp_client = make_kfp_client(env["kfp_host"], namespace, sa_token)
        upload_to_kfp(kfp_client, pipeline_yaml, pipeline_name, version_name, commit_sha, namespace)

    log.info("Promotion complete ✓")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="KFP v2 Build-Once Promote-Everywhere CI/CD (ORAS + event-driven)",
    )

    # Shared options
    parser.add_argument("--namespace",      required=True)
    parser.add_argument("--sa-token-path",
                        default="/var/run/secrets/kubernetes.io/serviceaccount/token")

    sub = parser.add_subparsers(dest="command", required=True)

    # ── build subcommand ────────────────────────────────────────────────────
    p_build = sub.add_parser("build", help="Compile and push (dev cluster)")
    p_build.add_argument("--repo-url",   required=True)
    p_build.add_argument("--branch",     required=True)
    p_build.add_argument("--commit-sha", required=True)
    p_build.add_argument("--before-sha", required=True)
    p_build.add_argument(
        "--token-path",
        default="/var/run/secrets/github-token/token",
    )
    p_build.set_defaults(func=cmd_build)

    # ── promote subcommand ──────────────────────────────────────────────────
    p_promo = sub.add_parser("promote", help="Pull replicated artifact and upload (stg/prd)")
    p_promo.add_argument("--pipeline-name", required=True,
                         help="OCI image name = pipeline folder name")
    p_promo.add_argument("--commit-sha",    required=True,
                         help="OCI tag = commit SHA (from Harbor webhook)")
    p_promo.set_defaults(func=cmd_promote)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
