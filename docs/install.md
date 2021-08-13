# Installation <!-- omit in toc -->

- [General Information](#general-information)
- [Supported Platforms](#supported-platforms)
  - [Linux](#linux)
  - [MacOS X](#macos-x)
  - [Windows (using WSL 2)](#windows-using-wsl-2)
  - [Windows (using Docker Desktop)](#windows-using-docker-desktop)
- [Installing shell completions](#installing-shell-completions)
- [A Note on Security](#a-note-on-security)
- [Experimental: Using Podman instead of Docker](#experimental-using-podman-instead-of-docker)


## General Information
`kamu` is a single-binary utility that comes bundled with most of its dependencies.

It relies on `docker` container-based virtualization to run such heavyweight frameworks like Spark, Flink, and Jupyter in isolated environments without needing you to install thousands of libraries and bloating your beloved laptop with their dependencies.

See also:
- [Experimental: Using Podman instead of Docker](#experimental-using-podman-instead-of-docker)


## Supported Platforms

### Linux
Linux is our primary target environment. We don't have packages for various Linux flavors yet, but since the tool is just a simple binary it's very easy to get started:
- Install `docker` using your distro's package manager (alternatively try [podman](#experimental-using-podman-instead-of-docker))
  - Make sure you can launch containers without `sudo` by following [official documentation](https://docs.docker.com/engine/install/linux-postinstall/)
- Download the latest version of `kamu` from the GitHub [release page](https://github.com/kamu-data/kamu-cli/releases/latest)
- Unpack and, `chmod +x` it
  ```
  tar -zxvf kamu-cli-x86_64-unknown-linux-gnu.tar.gz
  chmod +x kamu-cli-x86_64-unknown-linux-gnu/kamu
  ```
- Link it into your preferred location on your `PATH`
  ```
  cp kamu /usr/local/bin
  ```
- Use `kamu init --pull-images` to pre-load all Docker images

See also:
<!-- no toc -->
- [Installing shell completions](#installing-shell-completions)
- [A Note on Security](#a-note-on-security)
- [Experimental: Using Podman instead of Docker](#experimental-using-podman-instead-of-docker)

### MacOS X
Installing on MacOS X is very similar to Linux with following differences:
- Install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)
- `kamu` uses your system temp directory to store temporary files. This directory is not mounted into Docker's VM by default so you may need to use `VitualBox` to mount this directory into VM under the same path as on your host.
- Also consider allocating more CPUs and memory to the Docker VM.

See also:
<!-- no toc -->
- [Installing shell completions](#installing-shell-completions)
- [Experimental: Using Podman instead of Docker](#experimental-using-podman-instead-of-docker)

### Windows (using WSL 2)
- Install WSL following [these steps](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- Install Ubuntu distro from Microsoft Store
- Install `docker` (alternatively try [podman](#experimental-using-podman-instead-of-docker))
  - Make sure you can launch containers without `sudo` by following [official documentation](https://docs.docker.com/engine/install/linux-postinstall/)
- Download the latest version of `kamu` from the GitHub [release page](https://github.com/kamu-data/kamu-cli/releases/latest) (note that you should download Linux release)
- Unpack and, `chmod +x` it
- Link it into your preferred location on your `PATH`

See also:
<!-- no toc -->
- [Installing shell completions](#installing-shell-completions)
- [Experimental: Using Podman instead of Docker](#experimental-using-podman-instead-of-docker)

### Windows (using Docker Desktop)
* Install and run [Docker Desktop](https://docs.docker.com/docker-for-windows/install/).
  * It's a good idea to give the Docker's VM more CPU and RAM - you can do so in `VirtualBox`.
* Make sure that you can run `docker ps` successfully.
  * We recommend using `PowerShell` when working with `kamu`
* Download the latest `kamu` binary for Windows
* Add it to your `PATH` environment variable
* Use `kamu init --pull-images` to pre-load all Docker images

Docker Toolbox runs Docker in a Virtual Machine. This means to mount a file from your host file system into a Docker container the file first needs to be mounted into VM, so make sure all paths that `kamu` will need are mapped in VirtualBox VM settings.

> **Example:** Lets assume your workspace directory is `C:\Users\me\kamu`. When `kamu` runs it will detect that Docker runs in a VM it will convert it to `/c/Users/me/kamu`. So in your VM settings you may need to add a mapping from `C:\Users\me` to `/c/Users/me`.

## Installing shell completions
To be able to auto-complete the `kamu` commands please install completion scripts for the shell of your choosing. You can find detailed instructions by running `kamu completions --help`.

If you use `bash` add the following to your `~/.bashrc` file:

```bash
source <(kamu completions bash)
```

If you use `zsh` add the following to your `~/.zshrc` file:

```bash
autoload -U +X bashcompinit && bashcompinit
source <(kamu completions bash)
```


## A Note on Security
We take your security very seriously. Unfortunately the execution model of `docker` that involves running the daemon process under `root` violates the Unix user permission model. Combined with the step of making `docker` command [sudo-less](https://docs.docker.com/engine/install/linux-postinstall/) this means that any process you run under your user can potentially access the entire file system with root privileges. Until `docker` changes its runtime model, sudo-less access to Docker will remain a security threat.

On our side we are taking following measures to gain your trust:
* `kamu` and all of its components are open-source and available for review
* All of our `docker` images are based on reputable source images and are [available for review](https://github.com/kamu-data/kamu-images)
* When `kamu` starts `docker` containers it limits the scope of volumes it's mounting to a minimum. You can review the volume mounts by running `kamu` with `-v` flag or using `docker ps`.

To avoid all these issues please check out the experimental [`podman` support](#experimental-using-podman-instead-of-docker) - this container runtime operates in daemon-less and root-less mode, so it's fully compliant with the standard Unix permission model.


## Experimental: Using Podman instead of Docker 
We added an experimental support for [`podman`](https://podman.io/) - an alternative container runtime that fixes the shortcomings of `docker` [related to security](#a-note-on-security). We highly recommend you to give it a try, as we are planning to make it a default runtime in the near future.

In order to instruct `kamu` to use `podman` run:

```bash
kamu config set --user engine.runtime podman
kamu init --pull-images
```

Note: On some systems you need to separately install `podman-dnsname` package to allow contaiers to communicate with one another via hostnames. To check whether you have it run:

```bash
podman network create test
podman network ls
# NETWORK ID    NAME    VERSION  PLUGINS
# 9f86d081884c  test    0.4.0    bridge,portmap,firewall,tuning,dnsname
#                                                               ^^^ plugin installed
podman network prune
```
 
