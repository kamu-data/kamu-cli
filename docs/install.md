# Installation

## General Information

Dependencies:

- Kamu is a single-binary utility that comes bundled with most of its dependencies
- The tool is written in Scala/Java and requires Java Runtime Environment (JRE).
- It relies on `docker` container-based virtualization to run such heavyweight frameworks like Spark and Jupyter in isolated environments without needing you to install thousands of libraries and bloating your beloved laptop with their dependencies.


### A Note on Security

We take your security very seriously. Unfortunately the execution model of `docker` itself does not allow us to fully limit `kamu` to accessing only files you want to share with it, as it does not obey regular unix user permissions rules. Until `docker` changes its runtime model, giving any app the ability to start containers will remain a security threat.

We are taking following measures to gain your trust:
* `kamu` and all of its components are open-source and available for review
* `docker` images that we use also include only open-source components and are [available for review](https://github.com/kamu-data/kamu-images)
* When `kamu` starts `docker` containers it limits the scope of volumes it's mounting to a minimum. You can review the volume mounts by running `kamu` with `--debug` flag or using `docker ps`.

If there is anything else we can do to make you feel confident in using this tool - let us know!


## MacOS X via Homebrew

Install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/) and make sure it's running by executing some simple command like `docker ps`.

JRE should already be available in MacOS X, you can verify this by running `java -version`.

Kamu is distributed via [Homebrew](https://brew.sh/), a popular package manager for MacOS. Make sure it's installed and then do:

```sh
# Adds kamu tap (an extra repository of Homebrew formulas)
brew tap kamu-data/kamu
# Installs kamu-cli from above tap
brew install kamu
# Test
kamu version
```
