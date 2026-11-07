from __future__ import annotations

import json
import os
import random
import re
import subprocess
import sys
import time
from typing import Callable

ATTEMPTS = 10


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], check=check, capture_output=True, text=True)


def get_image_line_split(image: str) -> re.Pattern:
    # matches `image: "backend/auth:<tag>"` and the unquoted `image: flink/<job>:<tag>`
    return re.compile(r'(image: "?)' + re.escape(image) + r':([^"\s]+)')


def get_helm_values_tag(values: str, image: str) -> str:
    """The tag the image has in values.yaml, the image must appear exactly once."""
    matches = get_image_line_split(image).findall(values)
    if len(matches) != 1:
        raise ValueError(f"expected exactly one image line for {image}, found {len(matches)}")
    return matches[0][1]


def replace_tag(values: str, image: str, tag: str) -> str:
    """Replaces only the tag of the image's line, the rest of the file stays byte for byte the same."""
    return get_image_line_split(image).sub(lambda m: f"{m.group(1)}{image}:{tag}", values)


def is_older(sha: str, tag: str) -> bool:
    """Whether this push's commit is an ancestor of the given tag, i.e. an older build."""
    # a tag that isn't a commit yet (latest) is never newer
    if git("cat-file", "-e", f"{tag}^{{commit}}", check=False).returncode != 0:
        return False
    return git("merge-base", "--is-ancestor", sha, tag, check=False).returncode == 0


def plan(
    values: str, images: list[str], sha: str, is_older: Callable[[str, str], bool] = is_older
) -> tuple[str, list[str]]:
    """Returns the new values and the images whose tag is written.
    is_older is a parameter only so tests can answer it without a git repository."""
    tag = sha[:7]
    written = []
    for image in images:
        helm_values_tag = get_helm_values_tag(values, image)
        if helm_values_tag == tag:
            print(f"{image} is already at {tag}, don't need to rewrite")
            continue
        # a later push can deploy first, an older build must not replace a newer tag
        if is_older(sha, helm_values_tag):
            print(f"skip: this build's commit ({tag}) is older than the one already in values.yaml ({helm_values_tag}), "
                  f"we don't overwrite")
            continue
        values = replace_tag(values, image, tag)
        written.append(image)
    return values, written


def deploy(helm_values_path: str, images: list[str], sha: str, branch: str) -> None:
    tag = sha[:7]
    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")

    # pushes made close together race on the branch, a rejected push resets and redoes every edit on the new tip
    for _ in range(ATTEMPTS):
        git("pull", "--rebase", "--quiet", "origin", branch)
        with open(helm_values_path) as f:
            values, written = plan(f.read(), images, sha)
        if not written:
            print("nothing to deploy")
            return
        with open(helm_values_path, "w") as f:
            f.write(values)

        # one commit for the whole push, the deployed images are listed in the body
        git("commit", "-qa", "-m", f"deploy: {tag}", "-m", "\n".join(written))
        # a push made with GITHUB_TOKEN triggers no workflow, so this can't loop
        if git("push", "--quiet", "origin", f"HEAD:{branch}", check=False).returncode == 0:
            print(f"deployed {tag}: {', '.join(written)}")
            return
        git("reset", "--quiet", "--hard", f"origin/{branch}")
        # a random 1-5s wait, so runs that collided don't retry in lockstep
        time.sleep(random.randint(1, 5))

    sys.exit(f"could not push the tags after {ATTEMPTS} attempts")


def main() -> None:
    # every changed service was built, so all of them are deployed
    images = [s["image"] for s in json.loads(os.environ["SERVICES"])]
    deploy(os.environ["HELM_VALUES_PATH"], images, os.environ["GITHUB_SHA"], os.environ["GITHUB_REF_NAME"])


if __name__ == "__main__":
    main()
