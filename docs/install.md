# Installation

- [Installation](#installation)
  - [General Information](#general-information)
    - [A Note on Security](#a-note-on-security)
  - [Supported Platforms](#supported-platforms)
    - [Linux](#linux)
    - [MacOS X via Homebrew](#macos-x-via-homebrew)
    - [Windows](#windows)
      - [Known issues](#known-issues)
    - [Other platforms](#other-platforms)
  - [Installing shell completions](#installing-shell-completions)


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


## Supported Platforms

### Linux

We don't have packages for various Linux flavors yet, but the project is being developed on Linux and supports it well. Simply install `docker` and `jre`, download the binary, `chown +x` it, and link it into your preferred location on your `PATH`.

### MacOS X via Homebrew

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

### Windows

Windows is supported partially, see [known issues](#known-issues).

You should already have Java, but check `java -version` just in case.

Install Docker for Windows. If you are using Docker toolbox backed by VirtualBox it's a good idea to give the Docker's VM more CPU and RAM. Make sure that you can run `docker ps` successfully.

Download `kamu` binary and save it as `kamu.jar`. You should already be able to run it as `java -jar kamu.jar`.

If you decide to use `cygwin` as your shell you can create the following laucher script and add it to `PATH`:

```sh
#!/bin/bash

java -jar `cygpath -w /home/me/kamu/bin/kamu.jar` $@
```

#### Known issues
- Docker is quite slow under VirtualBox
- Default `cmd` shell doesn't understand ANSI colors
- SQL shell history doesn't work
- SQL shell arrow keys don't work
- SQL shell Input/output formatting is sometimes off

### Other platforms

In case you didn't find your OS/distribution in this list you can try to download `kamu` binary from our [releases page](https://github.com/kamu-data/kamu-cli/releases). It should run on most systems capable of running Java and Docker.


## Installing shell completions

To be able to auto-complete the `kamu` commands please install completion scripts for the shell of your choosing. You can find detailed instructions by running `kamu completion --help`.

For example if you use `bash` you can add following into your `~/.bashrc` file:

```bash
source <(kamu completion bash)
```
