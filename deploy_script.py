import os
import sys
import subprocess
import yaml
from pathlib import Path
import hashlib

def run_cmd(cmd):
    """Run a shell command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Command failed: {cmd}\n{result.stderr}")
        sys.exit(1)
    return result.stdout.strip()

def load_cluster_config(env):
    """Load cluster YAML config."""
    config_path = f"configs/{env}_clusters.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Cluster config not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)

def file_checksum(path):
    """Get SHA256 checksum of a file."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def get_changed_files(full_deploy):
    """Get list of changed files compared to main branch."""
    if full_deploy:
        print("📦 Full deployment mode — deploying all files.")
        files = [str(p) for p in Path(".").rglob("*") if p.suffix in (".py", ".ipynb")]
    else:
        print("🔍 Detecting changed files since main...")
        run_cmd("git fetch origin main")
        changed = run_cmd("git diff --name-only origin/main...HEAD")
        files = [f for f in changed.split("\n") if f.strip() and f.endswith((".py", ".ipynb", ".yaml"))]
    return files

def deploy_files(files, env):
    """Upload notebooks and Python files."""
    for file in files:
        if file.endswith((".py", ".ipynb")):
            databricks_path = f"/Shared/{env}/{Path(file).with_suffix('')}"
            print(f"⬆️ Uploading: {file} -> {databricks_path}")
            subprocess.run([
                "databricks", "workspace", "import", file, databricks_path, "--overwrite"
            ], check=True)

def deploy_cluster_if_changed(env, changed_files):
    """Deploy cluster only if config changed."""
    cluster_file = f"configs/{env}_cluster.yaml"
    hash_file = f".last_{env}_cluster_hash"

    if cluster_file not in changed_files and os.path.exists(hash_file):
        old_hash = Path(hash_file).read_text()
        new_hash = file_checksum(cluster_file)
        if old_hash == new_hash:
            print(f"⏩ No changes in {cluster_file}, skipping cluster creation.")
            return

    print(f"🚀 Creating/Updating cluster for {env}")
    config = load_cluster_config(env)
    cluster_json_path = f"/tmp/{env}_cluster.json"
    with open(cluster_json_path, "w") as f:
        yaml.dump(config, f)

    subprocess.run([
        "databricks", "clusters", "create", "--json-file", cluster_json_path
    ], check=True)

    # Save hash for future runs
    Path(hash_file).write_text(file_checksum(cluster_file))

if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "--env":
        print("Usage: python deploy_script.py --env <dev|test|uat|prod> [--full]")
        sys.exit(1)

    env = sys.argv[2]
    full_deploy = "--full" in sys.argv

    print(f"🚀 Starting deployment for {env}")
    changed_files = get_changed_files(full_deploy)

    if not changed_files:
        print("✅ No changes detected. Skipping deployment.")
        sys.exit(0)

    deploy_cluster_if_changed(env, changed_files)
    deploy_files(changed_files, env)

    print(f"✅ Deployment completed for {env}")
