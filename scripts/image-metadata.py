#!/usr/bin/env python3
"""Derive container metadata from the checked-out package and GitHub event."""

import json
import os
from pathlib import Path
import re
import subprocess
import tomllib


def metadata(repository, revision, version, event_name, event, ref):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Expected a full source commit SHA")
    # Docker tags cannot contain SemVer build metadata (+); reject instead of
    # silently mapping distinct versions to the same image tag.
    semver = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z.-]+))?"
    if not re.fullmatch(semver, version):
        raise ValueError("Package version must be major.minor.patch[-prerelease]")
    image = f"ghcr.io/{repository.lower()}"
    tags = [f"sha-{revision}"]
    publish = False
    image_version = f"{version}-dev.{revision}"
    if event_name == "release":
        release = event["release"]
        tag = release["tag_name"]
        normalized = tag.removeprefix("v")
        if normalized != version:
            raise ValueError("Release tag must match Cargo.toml and frontend/package.json")
        if release.get("draft") or event.get("action") != "published":
            raise ValueError("Only published releases can publish images")
        if release.get("prerelease") and "-" not in version:
            raise ValueError("Prereleases require a version suffix, for example 5.3.0-rc.1")
        tags += list(dict.fromkeys([version, tag]))
        if "-" not in version and not release.get("prerelease"):
            tags += ["stable", "latest"]
        image_version = version
        publish = True
    elif event_name == "push" and ref == "refs/heads/main":
        tags += ["dev"]
        publish = True
    if any(len(tag) > 128 for tag in tags):
        raise ValueError("Image tag exceeds Docker's 128-character limit")
    return {
        "image": image,
        "revision": revision,
        "source": f"https://github.com/{repository}",
        "version": image_version,
        "tags": " ".join(tags),
        "publish": str(publish).lower(),
    }


def main():
    cargo = tomllib.loads(Path("Cargo.toml").read_text())["package"]["version"]
    frontend = json.loads(Path("frontend/package.json").read_text())["version"]
    lock = tomllib.loads(Path("Cargo.lock").read_text())
    locked = next(p["version"] for p in lock["package"] if p["name"] == "cbzxl")
    if cargo != frontend or cargo != locked:
        raise ValueError("Cargo.toml, Cargo.lock and frontend/package.json versions must agree")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    values = metadata(
        os.environ["GITHUB_REPOSITORY"], revision, cargo,
        os.environ["GITHUB_EVENT_NAME"],
        json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()),
        os.environ["GITHUB_REF"],
    )
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for key, value in values.items():
            print(f"{key}={value}", file=output)


if __name__ == "__main__":
    main()
