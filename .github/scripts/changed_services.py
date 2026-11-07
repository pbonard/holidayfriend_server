from __future__ import annotations

import json
import os
import subprocess

SERVICES_FILE = ".github/services.json"
# a change to the pipeline itself rebuilds every service
PIPELINE_PATHS = (SERVICES_FILE, ".github/workflows/services.yml", ".github/scripts/")


def changed_files(before: str, sha: str) -> list[str] | None:
    known = subprocess.run(
        ["git", "cat-file", "-e", f"{before}^{{commit}}"], capture_output=True
    ).returncode == 0
    if not known:
        return None
    diff = subprocess.run(
        ["git", "diff", "--name-only", before, sha], check=True, capture_output=True, text=True
    )
    return diff.stdout.splitlines()


def select_services(services: list[dict], files: list[str] | None) -> list[dict]:
    if files is None or any(f.startswith(PIPELINE_PATHS) for f in files):
        return services
    return [s for s in services if any(f.startswith(tuple(s["paths"])) for f in files)]


def main() -> None:
    with open(SERVICES_FILE) as f:
        services = json.load(f)
    changed = select_services(services, changed_files(os.environ["BEFORE"], os.environ["GITHUB_SHA"]))

    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        out.write(f"services={json.dumps(changed)}\n")
    print("changed services:")
    for s in changed:
        print(f"  {s['name']}")


if __name__ == "__main__":
    main()
