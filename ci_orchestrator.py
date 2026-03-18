#!/usr/bin/env python3
import os
import subprocess
import sys
import yaml
from pathlib import Path

def run_cmd(cmd: list[str], hide_output: bool = False, input_text: str = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            cmd,
            check=True,
            capture_output=hide_output,
            text=True,
            input=input_text
        )
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(cmd)}\n{e.stderr}", file=sys.stderr)
        sys.exit(1)

def generate_crd_yaml(pipeline_name: str, namespace: str, schedule: str, oci_ref: str, short_sha: str) -> str:
    yaml_docs = []
    
    yaml_docs.append(f"""apiVersion: pipelines.kubeflow.org/v2beta1
kind: Pipeline
metadata:
  name: {pipeline_name}
  namespace: {namespace}
spec:
  displayName: "{pipeline_name}"
""")

    yaml_docs.append(f"""apiVersion: pipelines.kubeflow.org/v2beta1
kind: PipelineVersion
metadata:
  name: {pipeline_name}-{short_sha}
  namespace: {namespace}
spec:
  pipelineRef:
    name: {pipeline_name}
  displayName: "Commit {short_sha}"
  packageUrl: "http://{oci_ref}"
""")

    if schedule and schedule.lower() != "none":
        yaml_docs.append(f"""apiVersion: pipelines.kubeflow.org/v1beta1
kind: ScheduledWorkflow
metadata:
  name: {pipeline_name}-schedule
  namespace: {namespace}
spec:
  trigger:
    cronSchedule:
      cron: "{schedule}"
  workflow:
    pipelineVersionRef:
      name: {pipeline_name}-{short_sha}
""")
    
    return "\n---\n".join(yaml_docs)

def main():
    try:
        repo_url = os.environ["REPO_URL"]
        raw_branch_name = os.environ["BRANCH_NAME"]
        branch_name = raw_branch_name.removeprefix("refs/heads/")
        tenant_namespace = os.environ["TENANT_NAMESPACE"]
        commit_sha = os.environ["COMMIT_SHA"]
        short_sha = commit_sha[:7]
        oci_registry = os.environ["OCI_REGISTRY"]
        oci_repository = os.environ["OCI_REPOSITORY"]
        oci_username = os.environ["OCI_USERNAME"]
        oci_password = os.environ["OCI_PASSWORD"]
        gitops_repo_url = os.environ["GITOPS_REPO_URL"]
        git_token_path = "/var/run/secrets/github-token/token"
    except KeyError as e:
        print(f"Missing required environment variable: {e}", file=sys.stderr)
        sys.exit(1)

    with open(git_token_path, "r") as f:
        git_token = f.read().strip()

    # 1. Clone the ML Repository
    print("📥 Cloning ML Code Repository...")
    repo_url_https = repo_url.replace("https://", "")
    auth_repo_url = f"https://x-access-token:{git_token}@{repo_url_https}"
    
    workspace_dir = Path("/workspace")
    workspace_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(workspace_dir)
    
    run_cmd(["git", "clone", "--branch", branch_name, auth_repo_url, "."])

    # 2. Parse Configuration
    config_path = Path("project-config.yaml")
    if not config_path.exists():
        print("Error: project-config.yaml not found in repository root.", file=sys.stderr)
        sys.exit(1)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    generated_crds = []

    # 3. Compile and Push Pipelines
    for experiment in config.get("experiments", []):
        for pipeline in experiment.get("pipelines", []):
            p_name = pipeline["name"]
            p_folder = pipeline["folder"]
            p_schedule = pipeline.get("schedule", "")
            
            main_py_path = Path(f"pipelines/{p_folder}/main.py")
            if not main_py_path.exists():
                print(f"Warning: Expected pipeline file {main_py_path} does not exist. Skipping.", file=sys.stderr)
                continue

            print(f"⚙️ Processing pipeline: {p_name} ({p_folder})...")
            
            compiled_yaml = f"{p_name}.yaml"
            run_cmd(["kfp", "dsl", "compile", "--py", str(main_py_path), "--output", compiled_yaml])

            oci_ref = f"{oci_registry}/{oci_repository}/{p_name}:sha-{short_sha}"
            
            print(f"🐳 Pushing {p_name} to Harbor...")
            push_cmd = [
                "oras", "push", oci_ref,
                f"{compiled_yaml}:application/yaml",
                "--artifact-type", "application/vnd.kubeflow.pipeline.v2+yaml",
                "--username", oci_username,
                "--password-stdin",
                "--plain-http"
            ]
            run_cmd(push_cmd, input_text=oci_password)

            crd_content = generate_crd_yaml(p_name, tenant_namespace, p_schedule, oci_ref, short_sha)
            crd_filename = f"{p_name}-crd.yaml"
            with open(crd_filename, "w") as f:
                f.write(crd_content)
            
            generated_crds.append((p_name, crd_filename))

    if not generated_crds:
        print("No pipelines processed. Exiting.")
        sys.exit(0)

    # 4. Commit CRDs to GitOps Repo
    print("🐙 Cloning GitOps Infrastructure Repository...")
    gitops_https = gitops_repo_url.replace("https://", "")
    auth_gitops_url = f"https://x-access-token:{git_token}@{gitops_https}"
    
    gitops_dir = Path("/tmp/gitops-repo")
    run_cmd(["git", "clone", auth_gitops_url, str(gitops_dir)])

    for p_name, crd_filename in generated_crds:
        target_dir = gitops_dir / "apps" / tenant_namespace / p_name
        target_dir.mkdir(parents=True, exist_ok=True)
        
        os.rename(crd_filename, target_dir / "pipeline-crd.yaml")
        run_cmd(["git", "-C", str(gitops_dir), "add", f"apps/{tenant_namespace}/{p_name}/pipeline-crd.yaml"])

    run_cmd(["git", "-C", str(gitops_dir), "config", "user.name", "argo-ci-bot"])
    run_cmd(["git", "-C", str(gitops_dir), "config", "user.email", "ci-bot@platform.local"])

    diff_check = subprocess.run(["git", "-C", str(gitops_dir), "diff-index", "--quiet", "HEAD"])
    if diff_check.returncode == 0:
        print("✅ No changes detected in the CRDs. Exiting.")
        sys.exit(0)

    print("🚀 Pushing updated CRDs to GitOps repository...")
    run_cmd(["git", "-C", str(gitops_dir), "commit", "-m", f"chore: update pipelines to sha-{short_sha}"])
    run_cmd(["git", "-C", str(gitops_dir), "push", "origin", "main"])
    print("✅ Successfully pushed all CRDs to GitOps repository.")

if __name__ == "__main__":
    main()