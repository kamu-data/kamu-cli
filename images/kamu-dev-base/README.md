# Building `kamu` dev images

This can be useful if you want to provide an image from changes that are not already in the main branch.
For example, during the development of a feature.

⚠️ Please note: for flexibility, this image does NOT include any kamu workspace.

### Build

```shell
# During the release build, we will be using cached SQLX queries
make sqlx-local-clean

cd ./images/kamu-dev-base/
make image
# Outputs:
#
# ✨ Image successfully built:
# · ghcr.io/kamu-data/kamu-dev-base:feature-private-datasets-603bf0885
```

### Verification

```shell
# We're taking IMAGE from a previous command
IMAGE=ghcr.io/kamu-data/kamu-dev-base:feature-private-datasets-603bf0885

docker run --rm \ 
  ${IMAGE} \
  kamu version

# Outputs:
#
# ...
# gitSha: 603bf08854ad2f93f2c20070bbcd7e8c7c547173
# gitBranch: feature/private-datasets
#
```

### Upload

Or, you can also specify the desired branch:
```shell
make image-push
# Outputs:
#
# ✨ Image successfully built:
# · https://ghcr.io/kamu-data/kamu-dev-base:feature-private-datasets-603bf0885
# · https://ghcr.io/kamu-data/kamu-dev-base:feature-private-datasets-latest
```


### Running with workspace attached

⚠️ Make sure you are in the directory where `.kamu` is located.

```shell
docker run --rm -v ./.kamu:/opt/kamu/workspace/.kamu kamu-base:git kamu ls -o table

# Outputs datasets
```
