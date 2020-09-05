# Installation

- [Installation](#installation)
  - [General Information](#general-information)
    - [A Note on Security](#a-note-on-security)
  - [Supported Platforms](#supported-platforms)
    - [Linux](#linux)
    - [MacOS X](#macos-x)
    - [Windows (using Docker Desktop)](#windows-using-docker-desktop)
    - [Windows (using WSL 2)](#windows-using-wsl-2)
  - [Installing shell completions](#installing-shell-completions)


## General Information
Dependencies:

- `kamu` is a single-binary utility that comes bundled with most of its dependencies
- It relies on `docker` container-based virtualization to run such heavyweight frameworks like Spark, Flink, and Jupyter in isolated environments without needing you to install thousands of libraries and bloating your beloved laptop with their dependencies.


### A Note on Security
We take your security very seriously. Unfortunately the execution model of `docker` itself does not allow us to fully limit `kamu` to accessing only files you want to share with it, as it does not obey regular unix user permissions rules. Until `docker` changes its runtime model, giving any app the ability to start containers will remain a security threat.

We are taking following measures to gain your trust:
* `kamu` and all of its components are open-source and available for review
* All of our `docker` images are based on reputable source images and are [available for review](https://github.com/kamu-data/kamu-images)
* When `kamu` starts `docker` containers it limits the scope of volumes it's mounting to a minimum. You can review the volume mounts by running `kamu` with `-v` flag or using `docker ps`.

If there is anything else we can do to make you feel confident in using this tool - let us know!


## Supported Platforms

### Linux
Linux is our primary target environment. We don't have packages for various Linux flavors yet, but since the tool is just a simple binary it's very easy to get started:
- Install `docker` using your distro's package manager
- Make sure you can launch containers without `sudo` (see [this](https://docs.docker.com/engine/install/linux-postinstall/))
- Download the latest version of `kamu` from the GitHub release page
- Unpack and, `chown +x` it
- Link it into your preferred location on your `PATH`.
- Use `kamu init --pull-images` to pre-load all Docker images

See also:
* [Installing shell completions](#installing-shell-completions)

### MacOS X
Installing on MacOS X is very similar to Linux with following differences:
* You'll need to install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)
* `kamu` uses your system temp directory to store temporary files. This directory is not mounted into Docker's VM by default so you may need to use `VitualBox` to mount this directory into VM under the same path as on your host.
* Also consider allocating more CPUs and memory to the Docker VM.

### Windows (using Docker Desktop)
* Install [Docker Desktop](https://docs.docker.com/docker-for-windows/install/).
* Make sure that you can run `docker ps` successfully.
* It's a good idea to give the Docker's VM more CPU and RAM - you can do so in `VirtualBox`.
* Download the latest `kamu` binary for Windows
* Add it to your `PATH` environment variable
* Use `kamu init --pull-images` to pre-load all Docker images

Docker Toolbox runs Docker in a Virtual Machine. This means to mount a file from your host file system into a Docker container the file first needs to be mounted into VM, so make sure all paths that `kamu` will need are mapped in VirtualBox VM settings.

> **Example:** If you run the tool under `cygwin` your home directory might be in `C:\cygwin64`. When `kamu` runs it will resolve the system temporary folder as `C:\cygwin64\tmp` and when it detects that Docker runs in a VM it will map it to `/c/cygwin64/tmp`. So in your VM settings you will need to add a mapping from `C:\cygwin64` to `/c/cygwin64`.

Windows is fun...

### Windows (using WSL 2)
TODO: Please contribute instructions!

## Installing shell completions
To be able to auto-complete the `kamu` commands please install completion scripts for the shell of your choosing. You can find detailed instructions by running `kamu completions --help`.

For example if you use `bash` you can add following into your `~/.bashrc` file:

```bash
source <(kamu completions bash)
```
