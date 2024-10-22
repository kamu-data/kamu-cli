# Building `kamu` in Docker

This can be useful if you need a `kamu-cli` build, but don't have the desire/ability 
to roll out a build environment and all the tools.

⚠️ Please note: for flexibility, this image does NOT include any kamu workspace.

### Build

```shell
cd ./images/kamu-base-git/
docker build --no-cache -t kamu-base:git .
```

Or, you can also specify the desired branch:
```shell
docker build --no-cache --build-arg KAMU_BRANCH=<REPLACE_ME> -t kamu-base:<REPLACE_ME> .
```

### Verification

```shell
docker run --rm kamu-base:git kamu version

# Outputs the version information
```

### Running with workspace attached

⚠️ Make sure you are in the directory where `.kamu` is located.

```shell
docker run --rm -v ./.kamu:/opt/kamu/workspace/.kamu kamu-base:git kamu ls -o table

# Outputs datasets
```
