---
name: kamu-jupyter-demo-release-workflows
description: Jupyter demo release and multi-platform image workflow for Kamu CLI. Use when updating the Kamu demo Jupyter image, rustfs image, DEMO_VERSION, images/demo docker-compose versions, or manually building and pushing demo multi-arch images.
---

# Kamu Jupyter Demo Release Workflows

The Jupyter demo at `https://demo.kamu.dev` embeds `kamu-cli` and must be re-released whenever
protocol compatibility breaks.

The procedures are owned by `DEVELOPER.md`; follow them step by step. If they and this skill
disagree, `DEVELOPER.md` wins and this skill gets fixed.

| Task | Procedure |
|---|---|
| Releasing the demo images | [Jupyter Demo Release Procedure](../../../DEVELOPER.md#jupyter-demo-release-procedure) |
| Setting up `docker buildx` / QEMU | [Building Multi-platform Images](../../../DEVELOPER.md#building-multi-platform-images) |

## Agent checklist

- The only file edits are `DEMO_VERSION` in `images/demo/Makefile` and the matching `jupyter` and
  `rustfs` image tags in `images/demo/docker-compose.yml`. Confirm the two agree before anything
  is built.
- `make rustfs-multi-arch` and `make jupyter-multi-arch` build **and push** to a public registry.
  Pushing publishes: run them only with explicit user approval for that push.
- The Jupyter push needs a GitHub token with `write:packages` in `CR_PAT`, logged in to `ghcr.io`.
  Ask the user to set it up — never create, print, or store tokens yourself.
- Deployment to Kubernetes is outside this repository; stop after the images are published and
  report the pushed tags.

## What lives elsewhere

- Version bumps and releases of `kamu-cli` itself: `kamu-release-dependency-workflows`.
