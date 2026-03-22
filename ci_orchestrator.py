#!/usr/bin/env python3
import argparse
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import kfp
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    branch_name: str
    short_sha: str
    kfp_endpoint: str
    tenant_namespace: str
    repo_url: str = ""
    oci_registry: str = ""
    oci_repository: str = ""
    oci_username: str = ""
    oci_password: str = ""
    git_token_path: Path = Path("/var/run/secrets/github-token/token")
    workspace_dir: Path = Path("/workspace")
    config_file: str = "project-config.yaml"

    @classmethod
    def from_env(cls, command: str) -> "AppConfig":
        kfp_endpoint = os.environ.get("KFP_ENDPOINT")
        tenant_namespace = os.environ.get("TENANT_NAMESPACE")

        if not kfp_endpoint or not tenant_namespace:
            logger.critical("Infrastructure configuration failed. Missing KFP_ENDPOINT or TENANT_NAMESPACE.")
            sys.exit(1)

        try:
            config = cls(
                branch_name=os.environ["BRANCH_NAME"].removeprefix("refs/heads/"),
                short_sha=os.environ["COMMIT_SHA"][:7],
                kfp_endpoint=kfp_endpoint,
                tenant_namespace=tenant_namespace
            )
            if command == "plan":
                config.repo_url = os.environ["REPO_URL"].replace("https://", "")
                config.oci_registry = os.environ["OCI_REGISTRY"]
                config.oci_repository = os.environ["OCI_REPOSITORY"]
                config.oci_username = os.environ["OCI_USERNAME"]
                config.oci_password = os.environ["OCI_PASSWORD"]
            return config
        except KeyError as e:
            logger.critical(f"Missing required environment variable for command '{command}': {e}")
            sys.exit(1)


def execute_command(
    cmd: List[str], 
    cwd: Optional[Path] = None, 
    input_text: Optional[str] = None, 
    sensitive_strings: Optional[List[str]] = None
) -> str:
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=input_text,
            cwd=cwd
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        error_msg = f"Command execution failed.\nCMD: {' '.join(cmd)}\nSTDERR: {e.stderr}\nSTDOUT: {e.stdout}"
        if sensitive_strings:
            for secret in sensitive_strings:
                if secret and secret in error_msg:
                    error_msg = error_msg.replace(secret, "***REDACTED***")
        logger.error(error_msg)
        sys.exit(1)


class GitManager:
    def __init__(self, config: AppConfig):
        self.config = config

        logger.info(f"Configuring Git safe.directory for {self.config.workspace_dir}")
        execute_command([
            "git", "config", "--global", "--add", "safe.directory", str(self.config.workspace_dir)
        ])

    def _get_token(self) -> str:
        if not self.config.git_token_path.exists():
            logger.critical(f"Git token not found at {self.config.git_token_path}")
            sys.exit(1)
        with open(self.config.git_token_path, "r", encoding="utf-8") as f:
            return f.read().strip()

    def clone_repository(self) -> None:
        logger.info("Performing shallow clone of ML Code Repository...")
        token = self._get_token()
        auth_repo_url = f"https://x-access-token:{token}@{self.config.repo_url}"
        self.config.workspace_dir.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            "git", "clone", 
            "--depth", "2", 
            "--branch", self.config.branch_name, 
            auth_repo_url, 
            str(self.config.workspace_dir)
        ]
        execute_command(cmd, sensitive_strings=[token])

    def get_changed_files(self) -> List[str]:
        try:
            execute_command(["git", "rev-parse", "HEAD~1"], cwd=self.config.workspace_dir)
            cmd = ["git", "diff", "--name-only", "HEAD~1", "HEAD"]
        except SystemExit:
            logger.warning("HEAD~1 not found. Comparing against HEAD.")
            cmd = ["git", "show", "--name-only", "--format=", "HEAD"]
        
        output = execute_command(cmd, cwd=self.config.workspace_dir)
        return output.splitlines() if output else []


class PipelineManager:
    def __init__(self, config: AppConfig):
        self.config = config

        token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        token = token_path.read_text().strip() if token_path.exists() else None

        self.client = kfp.Client(
            host=config.kfp_endpoint,
            existing_token=token,
            namespace=config.tenant_namespace
        )

    def compile_pipeline(self, main_py_path: Path, compiled_yaml_path: Path) -> None:
        execute_command(
            ["kfp", "dsl", "compile", "--py", str(main_py_path.resolve()), "--output", str(compiled_yaml_path.resolve())],
            cwd=self.config.workspace_dir
        )

    def push_to_registry(self, p_name: str, compiled_yaml_path: Path) -> None:
        oci_ref = f"{self.config.oci_registry}/{self.config.oci_repository}/{p_name}:sha-{self.config.short_sha}"
        logger.info(f"Pushing {p_name} to Harbor at {oci_ref}...")
        
        cmd = [
            "oras", "push", oci_ref, f"{compiled_yaml_path.name}:application/yaml",
            "--artifact-type", "application/vnd.kubeflow.pipeline.v2+yaml",
            "--username", self.config.oci_username,
            "--password-stdin", "--plain-http"
        ]
        execute_command(
            cmd, 
            cwd=compiled_yaml_path.parent, 
            input_text=self.config.oci_password,
            sensitive_strings=[self.config.oci_password]
        )

    def upload_pipeline_version(self, p_name: str, compiled_yaml_path: Path) -> str:
        pipeline_id = self.client.get_pipeline_id(p_name)
        if not pipeline_id:
            kfp_pipeline = self.client.upload_pipeline(str(compiled_yaml_path), pipeline_name=p_name)
            pipeline_id = kfp_pipeline.pipeline_id
            
        logger.info(f"Uploading version sha-{self.config.short_sha} to Kubeflow API...")
        version_info = self.client.upload_pipeline_version(
            str(compiled_yaml_path), 
            pipeline_version_name=f"sha-{self.config.short_sha}", 
            pipeline_id=pipeline_id
        )
        return version_info.pipeline_version_id

    def get_pipeline_and_version_id(self, p_name: str, version_name: str) -> tuple[str, str]:
        pipeline_id = self.client.get_pipeline_id(p_name)
        if not pipeline_id:
            logger.critical(f"Pipeline '{p_name}' not found in Kubeflow.")
            sys.exit(1)
            
        response = self.client.list_pipeline_versions(pipeline_id=pipeline_id, page_size=100)
        versions = getattr(response, 'versions', []) or getattr(response, 'pipeline_versions', [])
        
        for v in versions:
            if getattr(v, 'name', '') == version_name:
                v_id = getattr(v, 'id', getattr(v, 'pipeline_version_id', None))
                if v_id:
                    return pipeline_id, v_id
                    
        logger.critical(f"Version '{version_name}' for pipeline '{p_name}' not found.")
        sys.exit(1)

    def ensure_experiment_exists(self, exp_name: str) -> str:
        try:
            exp = self.client.get_experiment(experiment_name=exp_name)
            return exp.experiment_id
        except ValueError:
            exp = self.client.create_experiment(name=exp_name)
            return exp.experiment_id


def load_parameters(params_path: Path) -> Dict[str, Any]:
    if not params_path.exists():
        return {}
    with open(params_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def determine_build_targets(changed_files: List[str], project_config: Dict[str, Any]) -> Set[str]:
    # Global triggers that necessitate a rebuild of all pipelines
    global_triggers = ("components/", "utils/", "project-config.yaml")
    if any(f.startswith(global_triggers) for f in changed_files):
        logger.info("Global dependency changed. Flagging all pipelines for rebuild.")
        all_pipelines = set()
        for exp in project_config.get("experiments", []):
            for p in exp.get("pipelines", []):
                all_pipelines.add(p["name"])
        return all_pipelines

    # Targeted triggers based on the strict naming convention
    targeted_pipelines = set()
    for f in changed_files:
        if f.startswith("pipelines/"):
            parts = f.split("/")
            if len(parts) >= 2:
                targeted_pipelines.add(parts[1])
    return targeted_pipelines


def handle_plan_command(args: argparse.Namespace, config: AppConfig) -> None:
    git_manager = GitManager(config)
    kfp_manager = PipelineManager(config)

    git_manager.clone_repository()
    changed_files = git_manager.get_changed_files()
    
    config_file_path = config.workspace_dir / config.config_file
    if not config_file_path.exists():
        logger.critical(f"Configuration file {config_file_path} not found.")
        sys.exit(1)

    with open(config_file_path, "r", encoding="utf-8") as f:
        project_config = yaml.safe_load(f) or {}

    pipelines_to_build = determine_build_targets(changed_files, project_config)
    execution_plan = []

    for experiment in project_config.get("experiments", []):
        exp_name = f"{experiment['name']}-{config.branch_name}"
        exp_id = kfp_manager.ensure_experiment_exists(exp_name)

        for pipeline in experiment.get("pipelines", []):
            p_name = pipeline["name"]
            p_schedule = pipeline.get("schedule")
            
            # Strict Convention Routing
            pipeline_dir = config.workspace_dir / f"pipelines/{p_name}"
            main_py_path = pipeline_dir / "main.py"
            params_yaml_path = pipeline_dir / "parameters.yaml"
            compiled_yaml_path = config.workspace_dir / f"{p_name}.yaml"
            
            if p_name not in pipelines_to_build:
                logger.info(f"Skipping '{p_name}': No relevant changes detected.")
                continue

            if not main_py_path.exists():
                logger.warning(f"Expected entrypoint {main_py_path} does not exist. Skipping.")
                continue

            logger.info(f"Planning pipeline: {p_name}")
            p_params = load_parameters(params_yaml_path)
            
            kfp_manager.compile_pipeline(main_py_path, compiled_yaml_path)
            kfp_manager.push_to_registry(p_name, compiled_yaml_path)
            version_id = kfp_manager.upload_pipeline_version(p_name, compiled_yaml_path)

            if p_schedule and p_schedule.lower() != "none":
                logger.info(f"Syncing Recurring Run for {p_name} with schedule: {p_schedule}")
                pipeline_id = kfp_manager.client.get_pipeline_id(p_name)
                kfp_manager.client.create_recurring_run(
                    experiment_id=exp_id,
                    job_name=f"{p_name}-schedule",
                    cron_expression=p_schedule,
                    pipeline_id=pipeline_id,
                    version_id=version_id,
                    params=p_params
                )

            execution_plan.append({
                "pipeline_name": p_name,
                "experiment_id": exp_id,
                "parameters_json": json.dumps(p_params)
            })

    args.out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_file, "w", encoding="utf-8") as f:
        json.dump(execution_plan, f, indent=2)
    
    print(json.dumps(execution_plan))


def handle_run_command(args: argparse.Namespace, config: AppConfig) -> None:
    kfp_manager = PipelineManager(config)
    p_name = args.pipeline_name
    version_name = f"sha-{config.short_sha}"
    
    pipeline_id, version_id = kfp_manager.get_pipeline_and_version_id(p_name, version_name)
    params = json.loads(args.parameters_json) if args.parameters_json else {}

    logger.info(f"Triggering immediate run for {p_name} (Version: {version_name})...")
    kfp_manager.client.run_pipeline(
        experiment_id=args.experiment_id,
        job_name=f"{p_name}-run-{config.short_sha}",
        pipeline_id=pipeline_id,
        version_id=version_id,
        params=params
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="KFP CI/CD Engine for Argo Workflows")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Compile pipelines, push to OCI, and generate JSON execution plan")
    plan_parser.add_argument("--out-file", type=Path, default=Path("/workspace/execution-plan.json"))

    run_parser = subparsers.add_parser("run", help="Trigger a specific KFP pipeline run")
    run_parser.add_argument("--pipeline-name", required=True)
    run_parser.add_argument("--experiment-id", required=True)
    run_parser.add_argument("--parameters-json", type=str, default="{}")

    args = parser.parse_args()
    config = AppConfig.from_env(args.command)

    if args.command == "plan":
        handle_plan_command(args, config)
    elif args.command == "run":
        handle_run_command(args, config)


if __name__ == "__main__":
    main()