---
name: kamu-jupyter-demo-release-workflows
description: Jupyter demo release and multi-platform image workflow for Kamu CLI. Use when updating the Kamu demo Jupyter image, rustfs image, DEMO_VERSION, images/demo docker-compose versions, or manually building and pushing demo multi-arch images.
---

# Kamu Jupyter Demo Release Workflows

Use this skill when the Jupyter demo at `https://demo.kamu.dev` needs a new image release, especially after protocol compatibility changes.

## Jupyter Demo Release

### 1. Update Versions

- Increment `DEMO_VERSION` in `images/demo/Makefile`.
- Set the same version for `jupyter` and `rustfs` images in `images/demo/docker-compose.yml`.

### 2. Prepare Datasets

- Run `make clean`.
- Run `make data` to prepare example datasets for the `rustfs` image.

### 3. Configure Docker Buildx

- Prepare Docker buildx for multi-platform images (see Multi-Platform Image Setup section below if not already configured).

### 4. Build and Push Images

- Run `make rustfs-multi-arch` to build and push the multi-arch `rustfs` image.
- **Always configure a GitHub package token with `write:packages` permission before pushing the Jupyter image.** This is required for every push.
- Run `make jupyter-multi-arch` to build and push the multi-arch `jupyter` image.

### 5. Deploy

- Deploy the new image to the Kubernetes environment after image publication. Deployment steps are specific to your infrastructure and outside the scope of this workflow.

## Multi-Platform Image Setup

Create a local buildx builder when needed:

```sh
docker buildx create --use --name multi-arch-builder
```

For QEMU emulation bootstrap on Linux:

```sh
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```

## Validation

- Confirm the image versions match between `images/demo/Makefile` and `images/demo/docker-compose.yml`.
- Confirm required images were pushed before deployment steps.
- Finish code/documentation changes with `cargo fmt` and `make clippy` when the repo has Rust-relevant edits.
