#!/bin/sh
# shellcheck shell=dash
#
# This script downloads and installs the latest available release of kamu-cli.
#
# Installer script inspired by:
# - https://sh.rustup.rs
# - https://raw.githubusercontent.com/wasmerio/wasmer-install/master/install.sh

set -eu

#########################################################################################
# Config
#########################################################################################

KAMU_DOCS_INSTALL_URL="https://docs.kamu.dev/cli/get-started/installation/"

KAMU_VERSION="${KAMU_VERSION:-}"
KAMU_ALIAS="${KAMU_ALIAS:-kamu}"
KAMU_INSTALL_PATH="${KAMU_INSTALL_PATH:-${HOME}/.local/bin/${KAMU_ALIAS}}"
KAMU_LIBC="${KAMU_LIBC:-}"
KAMU_RELEASES_URL="${KAMU_RELEASES_URL:-https://github.com/kamu-data/kamu-cli/releases}"
KAMU_DOWNLOADS_URL="${KAMU_DOWNLOADS_URL:-https://github.com/kamu-data/kamu-cli/releases/download}"

#########################################################################################
# Main
#########################################################################################


usage() {
    printf "kamu-install.sh
The installer for kamu-cli

USAGE:
    kamu-install.sh [FLAGS] [OPTIONS]

FLAGS:
    -h, --help              Prints help information
    --allow-downgrade       Allow downgrading to an older version
    --reinstall             Force re-installation

ENV VARS:
    KAMU_VERSION            Use to specify a version to be installed
    KAMU_ALIAS              Use to install the executable under a different name
    KAMU_INSTALL_PATH       Use to override the installation path
    KAMU_LIBC               Use 'gnu' (default) or 'musl' libc on Linux
" 1>&2
}

#########################################################################################

main() {
    local allow_downgrade=0
    local reinstall=0
    local need_tty=yes
    for arg in "$@"; do
        case "$arg" in
            --help)
                usage
                exit 0
                ;;
            --allow-downgrade)
                allow_downgrade=1
                ;;
            --reinstall)
                reinstall=1
                ;;
            *)
                OPTIND=1
                if [ "${arg%%--*}" = "" ]; then
                    # Long option (other than --help);
                    # don't attempt to interpret it.
                    continue
                fi
                while getopts :hy sub_arg "$arg"; do
                    case "$sub_arg" in
                        h)
                            usage
                            exit 0
                            ;;
                        y)
                            # user wants to skip the prompt - we don't need /dev/tty
                            need_tty=no
                            ;;
                        *)
                            err "Invalid options: ${arg}"
                            ;;
                        esac
                done
                ;;
        esac
    done

    print_header

    download_file --check
    need_cmd basename
    need_cmd dirname
    need_cmd uname
    need_cmd mktemp
    need_cmd chmod
    need_cmd mkdir
    need_cmd rm
    need_cmd rmdir
    need_cmd tar

    # Determine install path
    local _install_path="$KAMU_INSTALL_PATH"
    local _install_dir=$(dirname $_install_path)
    info_val "Install path" "$_install_path"

    # Detect architecture
    get_architecture || return 1
    local _arch="$RETVAL"
    assert_nz "$_arch" "arch"
    case "$_arch" in
        *windows*)
            err "Windows is not yet well supported - try installing under WSL2 instead. See instructions at: ${KAMU_DOCS_INSTALL_URL}"
            ;;
    esac
    info_val "Your architecture" $_arch

    # Determine installed version
    kamu_installed_version _installed_version "$_install_path"
    if [ ! -z $_installed_version ]; then
        info_val "Installed version" $_installed_version
    fi

    # Determine version to install
    if [ -z $KAMU_VERSION ]; then
        kamu_latest_version _to_install_version
        info_val "Latest version" $_to_install_version
    else
        _to_install_version="$KAMU_VERSION"
        info_val "Specified version" $_to_install_version
    fi

    # Should we install?
    if [ ! -z $_installed_version ]; then
        _version_cmp=$(semver_compare $_installed_version $_to_install_version)
        case $_version_cmp in
            # installed = to_install
            0)
                if [ $reinstall -eq 0 ]; then
                    if [ -z $KAMU_VERSION ]; then
                        warn "You already have the latest version"
                    else
                        warn "Specified version is already installed"
                    fi
                    return 0
                fi
                ;;
            # installed > to_install
            1)
                if [ $reinstall -eq 0 ] && [ $allow_downgrade -eq 0 ]; then
                    warn "You already have a later version, specify --allow-downgrade if you want install anyway"
                    return 1
                fi
                ;;
            # installed < to_install
            -1)
                # moving on
                ;;
        esac
    fi

    # Check for conflicting commands
    local _bin_name=$(basename ${_install_path})
    if check_cmd which; then
        if which $_bin_name &>/dev/null; then
            local _conflict_path=$(which ${_bin_name})
            if [ "$_conflict_path" != "$_install_path" ]; then
                warn "Potentially conflicting binary found in PATH: ${_conflict_path}"
            fi
        fi
    fi

    # Download
    local _artifact="kamu-cli-${_arch}.tar.gz"
    local _url="${KAMU_DOWNLOADS_URL}/v${_to_install_version}/${_artifact}"
    local _tmpdir="$(ensure mktemp -d)"
    ensure mkdir -p "$_tmpdir"
    local _archive="${_tmpdir}/${_artifact}"

    say "Downloading release from ${_url} ..."
    ensure download_file "$_url" "$_archive" "$_arch"

    # Unpack
    say "Installing release ..."
    ensure tar -xf "${_archive}" -C "${_tmpdir}"
    rm "${_archive}"

    # Install
    local _bin="${_tmpdir}/kamu-cli-${_arch}/kamu"
    ensure chmod +x "$_bin"
    mkdir -p $_install_dir
    mv "$_bin" "${_install_path}"

    # Clean up
    rm -rf "$_tmpdir"

    printf "${green}Installation successful${reset}\n" 1>&2

    # TODO: Update profile automatically
    case $PATH in
        *$_install_dir*)
            ;;
        *)
            warn "The $_install_dir directory is not on your PATH - you may need to add it manually to your shell's profile (.bashrc | .zshrc | ...)"
            ;;
    esac

    # TODO: Install shell completions automatically
    warn "Consider installing shell completions (see: kamu completions --help)"
}

#########################################################################################

# ╦╔═ ╔═╗ ╔╦╗ ╦ ╦
# ╠╩╗ ╠═╣ ║║║ ║ ║
# ╩ ╩ ╩ ╩ ╩ ╩ ╚═╝

print_header() {
    logoclr="${cyanbr}"
    printf \
"${logoclr}${cyan}${bold}
  ██╗  ██╗ █████╗ ███╗   ███╗██╗   ██╗
  ██║ ██╔╝██╔══██╗████╗ ████║██║   ██║
  █████╔╝ ███████║██╔████╔██║██║   ██║
  ██╔═██╗ ██╔══██║██║╚██╔╝██║██║   ██║
  ██║  ██╗██║  ██║██║ ╚═╝ ██║╚██████╔╝
  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝ ╚═════╝
      Planet-scale data pipeline
${logoclr}${reset}
${logoclr}  ${cyan}${bold}Website:${reset}${cyan} https://kamu.dev${reset}
${logoclr}  ${cyan}${bold}Docs:${reset}${cyan} https://docs.kamu.dev/cli/${reset}
${logoclr}  ${cyan}${bold}Discord:${reset}${cyan} https://discord.gg/nU6TXRQNXC${reset}
${logoclr}  ${cyan}${bold}Repo:${reset}${cyan} https://github.com/kamu-data/kamu-cli${reset}

" 1>&2
}

#########################################################################################

kamu_latest_version() {
    download_json release_json "${KAMU_RELEASES_URL}/latest" || return 1
    version=$(echo "${release_json}" | tr -s '\n' ' ' | sed 's/.*"tag_name":"//' | sed 's/".*//')
    version=$(echo $version | sed 's/^v//')
    eval "$1='$version'"
    return 0
}

#########################################################################################

kamu_installed_version() {
    _bin="$2"
    if ! check_cmd "$_bin"; then
        version=""
    else
        # TODO: As of 2023-07-28 the `--version` flag is considered deprecated.
        # We should switch to `version` command after a grace period of ~6 months.
        version=$($_bin --version)
        version=$(echo $version | sed 's/^kamu //')
    fi
    eval "$1='$version'"
    return 0
}

#########################################################################################
# Colors
#########################################################################################

bold=""
reset=""
red=""
green=""
yellow=""
magenta=""
cyan=""
cyanbr=""

if [ -t 2 ]; then
    if [ "${TERM+set}" = 'set' ]; then
        case "$TERM" in
            xterm*|rxvt*|urxvt*|linux*|vt*)
                bold=$(tput bold)
                reset="\033[0m"
                red="\033[31m"
                green="\033[32m"
                yellow="\033[33m"
                magenta="\033[34m"
                cyan="\033[36m"
                cyanbr="\033[96m"
            ;;
        esac
    fi
fi

#########################################################################################
# Sh Utils
#########################################################################################

say() {
    printf '%s\n' "$1" 1>&2
}

warn() {
    printf "${yellow}%s${reset}\n" "$1" 1>&2
}

err() {
    printf "${red}%s${reset}\n" "$1" 1>&2
    exit 1
}

info_val() {
    printf "${bold}%s${reset} %s\n" "$1:" "$2" 1>&2
}

need_cmd() {
    if ! check_cmd "$1"; then
        err "need '$1' (command not found)"
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

assert_nz() {
    if [ -z "$1" ]; then err "assert_nz $2"; fi
}

# Run a command that should never fail. If the command fails execution
# will immediately terminate with an error showing the failing
# command.
ensure() {
    if ! "$@"; then err "command failed: $*"; fi
}

# This is just for indicating that commands' results are being
# intentionally ignored. Usually, because it's being executed
# as part of error handling.
ignore() {
    "$@"
}

#########################################################################################
# Environment
#########################################################################################

check_proc() {
    # Check for /proc by looking for the /proc/self/exe link
    # This is only run on Linux
    if ! test -L /proc/self/exe ; then
        err "fatal: Unable to find /proc/self/exe.  Is /proc mounted?  Installation cannot proceed without /proc."
    fi
}

#########################################################################################

get_bitness() {
    need_cmd head
    # Architecture detection without dependencies beyond coreutils.
    # ELF files start out "\x7fELF", and the following byte is
    #   0x01 for 32-bit and
    #   0x02 for 64-bit.
    # The printf builtin on some shells like dash only supports octal
    # escape sequences, so we use those.
    local _current_exe_head
    _current_exe_head=$(head -c 5 /proc/self/exe )
    if [ "$_current_exe_head" = "$(printf '\177ELF\001')" ]; then
        echo 32
    elif [ "$_current_exe_head" = "$(printf '\177ELF\002')" ]; then
        echo 64
    else
        err "unknown platform bitness"
    fi
}

#########################################################################################

is_host_amd64_elf() {
    need_cmd head
    need_cmd tail
    # ELF e_machine detection without dependencies beyond coreutils.
    # Two-byte field at offset 0x12 indicates the CPU,
    # but we're interested in it being 0x3E to indicate amd64, or not that.
    local _current_exe_machine
    _current_exe_machine=$(head -c 19 /proc/self/exe | tail -c 1)
    [ "$_current_exe_machine" = "$(printf '\076')" ]
}

#########################################################################################

get_endianness() {
    local cputype=$1
    local suffix_eb=$2
    local suffix_el=$3

    # detect endianness without od/hexdump, like get_bitness() does.
    need_cmd head
    need_cmd tail

    local _current_exe_endianness
    _current_exe_endianness="$(head -c 6 /proc/self/exe | tail -c 1)"
    if [ "$_current_exe_endianness" = "$(printf '\001')" ]; then
        echo "${cputype}${suffix_el}"
    elif [ "$_current_exe_endianness" = "$(printf '\002')" ]; then
        echo "${cputype}${suffix_eb}"
    else
        err "unknown platform endianness"
    fi
}

#########################################################################################

get_architecture() {
    local _ostype _cputype _bitness _arch _clibtype
    _ostype="$(uname -s)"
    _cputype="$(uname -m)"
    _clibtype="gnu"

    if [ "$_ostype" = Linux ]; then
        if [ "$(uname -o)" = Android ]; then
            _ostype=Android
        fi
        if [ -n "$KAMU_LIBC" ]; then
            _clibtype="$KAMU_LIBC"
        elif ldd --version 2>&1 | grep -q 'musl'; then
            _clibtype="musl"
        fi
    fi

    if [ "$_ostype" = Darwin ] && [ "$_cputype" = i386 ]; then
        # Darwin `uname -m` lies
        if sysctl hw.optional.x86_64 | grep -q ': 1'; then
            _cputype=x86_64
        fi
    fi

    if [ "$_ostype" = SunOS ]; then
        # Both Solaris and illumos presently announce as "SunOS" in "uname -s"
        # so use "uname -o" to disambiguate.  We use the full path to the
        # system uname in case the user has coreutils uname first in PATH,
        # which has historically sometimes printed the wrong value here.
        if [ "$(/usr/bin/uname -o)" = illumos ]; then
            _ostype=illumos
        fi

        # illumos systems have multi-arch userlands, and "uname -m" reports the
        # machine hardware name; e.g., "i86pc" on both 32- and 64-bit x86
        # systems.  Check for the native (widest) instruction set on the
        # running kernel:
        if [ "$_cputype" = i86pc ]; then
            _cputype="$(isainfo -n)"
        fi
    fi

    case "$_ostype" in

        Android)
            _ostype=linux-android
            ;;

        Linux)
            check_proc
            _ostype=unknown-linux-$_clibtype
            _bitness=$(get_bitness)
            ;;

        FreeBSD)
            _ostype=unknown-freebsd
            ;;

        NetBSD)
            _ostype=unknown-netbsd
            ;;

        DragonFly)
            _ostype=unknown-dragonfly
            ;;

        Darwin)
            _ostype=apple-darwin
            ;;

        illumos)
            _ostype=unknown-illumos
            ;;

        MINGW* | MSYS* | CYGWIN* | Windows_NT)
            _ostype=pc-windows-gnu
            ;;

        *)
            err "unrecognized OS type: $_ostype"
            ;;

    esac

    case "$_cputype" in

        i386 | i486 | i686 | i786 | x86)
            _cputype=i686
            ;;

        xscale | arm)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            fi
            ;;

        armv6l)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        armv7l | armv8l)
            _cputype=armv7
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        aarch64 | arm64)
            _cputype=aarch64
            ;;

        x86_64 | x86-64 | x64 | amd64)
            _cputype=x86_64
            ;;

        mips)
            _cputype=$(get_endianness mips '' el)
            ;;

        mips64)
            if [ "$_bitness" -eq 64 ]; then
                # only n64 ABI is supported for now
                _ostype="${_ostype}abi64"
                _cputype=$(get_endianness mips64 '' el)
            fi
            ;;

        ppc)
            _cputype=powerpc
            ;;

        ppc64)
            _cputype=powerpc64
            ;;

        ppc64le)
            _cputype=powerpc64le
            ;;

        s390x)
            _cputype=s390x
            ;;
        riscv64)
            _cputype=riscv64gc
            ;;
        *)
            err "unknown CPU type: $_cputype"

    esac

    # Detect 64-bit linux with 32-bit userland
    if [ "${_ostype}" = unknown-linux-gnu ] && [ "${_bitness}" -eq 32 ]; then
        case $_cputype in
            x86_64)
                if [ -n "${KAMU_CPUTYPE:-}" ]; then
                    _cputype="$KAMU_CPUTYPE"
                else {
                    # 32-bit executable for amd64 = x32
                    if is_host_amd64_elf; then {
                         echo "This host is running an x32 userland; as it stands, x32 support is poor," 1>&2
                         echo "and there isn't a native toolchain -- you will have to install" 1>&2
                         echo "multiarch compatibility with i686 and/or amd64, then select one" 1>&2
                         echo "by re-running this script with the KAMU_CPUTYPE environment variable" 1>&2
                         echo "set to i686 or x86_64, respectively." 1>&2
                         echo 1>&2
                         exit 1
                    }; else
                        _cputype=i686
                    fi
                }; fi
                ;;
            mips64)
                _cputype=$(get_endianness mips '' el)
                ;;
            powerpc64)
                _cputype=powerpc
                ;;
            aarch64)
                _cputype=armv7
                if [ "$_ostype" = "linux-android" ]; then
                    _ostype=linux-androideabi
                else
                    _ostype="${_ostype}eabihf"
                fi
                ;;
            riscv64gc)
                err "riscv64 with 32-bit userland unsupported"
                ;;
        esac
    fi

    # Detect armv7 but without the CPU features Rust needs in that build,
    # and fall back to arm.
    # See https://github.com/rust-lang/rustup.rs/issues/587.
    if [ "$_ostype" = "unknown-linux-gnueabihf" ] && [ "$_cputype" = armv7 ]; then
        if ensure grep '^Features' /proc/cpuinfo | grep -q -v neon; then
            # At least one processor does not have NEON.
            _cputype=arm
        fi
    fi

    _arch="${_cputype}-${_ostype}"

    RETVAL="$_arch"
}

#########################################################################################
# Downloads
#########################################################################################

download_json() {
  local url="$2"

  # echo "Fetching $url.."
  if test -x "$(command -v curl)"; then
    response=$(curl -s -L -w 'HTTPSTATUS:%{http_code}' -H 'Accept: application/json' "$url")
    body=$(echo "$response" | sed -e 's/HTTPSTATUS\:.*//g')
    code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  elif test -x "$(command -v wget)"; then
    temp=$(mktemp)
    body=$(wget -q --header='Accept: application/json' -O - --server-response "$url" 2>"$temp")
    code=$(awk '/^  HTTP/{print $2}' <"$temp" | tail -1)
    rm "$temp"
  else
    err "Neither curl nor wget was available to perform http requests"
    return 1
  fi
  if [ "$code" != 200 ]; then
    err "File download failed with code $code"
    return 1
  fi

  eval "$1='$body'"
  return 0
}

#########################################################################################

# This wraps curl or wget. Try curl first, if not installed,
# use wget instead.
download_file() {
    local _dld
    local _ciphersuites
    local _err
    local _status
    local _retry
    if check_cmd curl; then
        _dld=curl
    elif check_cmd wget; then
        _dld=wget
    else
        _dld='curl or wget' # to be used in error message of need_cmd
    fi

    if [ "$1" = --check ]; then
        need_cmd "$_dld"
    elif [ "$_dld" = curl ]; then
        check_curl_for_retry_support
        _retry="$RETVAL"
        get_ciphersuites_for_curl
        _ciphersuites="$RETVAL"
        if [ -n "$_ciphersuites" ]; then
            _err=$(curl $_retry --proto '=https' --tlsv1.2 --ciphers "$_ciphersuites" --silent --show-error --fail --location "$1" --output "$2" 2>&1)
            _status=$?
        else
            echo "Warning: Not enforcing strong cipher suites for TLS, this is potentially less secure"
            if ! check_help_for "$3" curl --proto --tlsv1.2; then
                echo "Warning: Not enforcing TLS v1.2, this is potentially less secure"
                _err=$(curl $_retry --silent --show-error --fail --location "$1" --output "$2" 2>&1)
                _status=$?
            else
                _err=$(curl $_retry --proto '=https' --tlsv1.2 --silent --show-error --fail --location "$1" --output "$2" 2>&1)
                _status=$?
            fi
        fi
        if [ -n "$_err" ]; then
            echo "$_err" >&2
            if echo "$_err" | grep -q 404$; then
                err "installer for platform '$3' not found, this may be unsupported"
            fi
        fi
        return $_status
    elif [ "$_dld" = wget ]; then
        if [ "$(wget -V 2>&1|head -2|tail -1|cut -f1 -d" ")" = "BusyBox" ]; then
            echo "Warning: using the BusyBox version of wget.  Not enforcing strong cipher suites for TLS or TLS v1.2, this is potentially less secure"
            _err=$(wget "$1" -O "$2" 2>&1)
            _status=$?
        else
            get_ciphersuites_for_wget
            _ciphersuites="$RETVAL"
            if [ -n "$_ciphersuites" ]; then
                _err=$(wget --https-only --secure-protocol=TLSv1_2 --ciphers "$_ciphersuites" "$1" -O "$2" 2>&1)
                _status=$?
            else
                echo "Warning: Not enforcing strong cipher suites for TLS, this is potentially less secure"
                if ! check_help_for "$3" wget --https-only --secure-protocol; then
                    echo "Warning: Not enforcing TLS v1.2, this is potentially less secure"
                    _err=$(wget "$1" -O "$2" 2>&1)
                    _status=$?
                else
                    _err=$(wget --https-only --secure-protocol=TLSv1_2 "$1" -O "$2" 2>&1)
                    _status=$?
                fi
            fi
        fi
        if [ -n "$_err" ]; then
            echo "$_err" >&2
            if echo "$_err" | grep -q ' 404 Not Found$'; then
                err "installer for platform '$3' not found, this may be unsupported"
            fi
        fi
        return $_status
    else
        err "Unknown downloader"   # should not reach here
    fi
}

#########################################################################################

check_help_for() {
    local _arch
    local _cmd
    local _arg
    _arch="$1"
    shift
    _cmd="$1"
    shift

    local _category
    if "$_cmd" --help | grep -q 'For all options use the manual or "--help all".'; then
      _category="all"
    else
      _category=""
    fi

    case "$_arch" in

        *darwin*)
        if check_cmd sw_vers; then
            case $(sw_vers -productVersion) in
                10.*)
                    # If we're running on macOS, older than 10.13, then we always
                    # fail to find these options to force fallback
                    if [ "$(sw_vers -productVersion | cut -d. -f2)" -lt 13 ]; then
                        # Older than 10.13
                        echo "Warning: Detected macOS platform older than 10.13"
                        return 1
                    fi
                    ;;
                11.*)
                    # We assume Big Sur will be OK for now
                    ;;
                *)
                    # Unknown product version, warn and continue
                    echo "Warning: Detected unknown macOS major version: $(sw_vers -productVersion)"
                    echo "Warning TLS capabilities detection may fail"
                    ;;
            esac
        fi
        ;;

    esac

    for _arg in "$@"; do
        if ! "$_cmd" --help $_category | grep -q -- "$_arg"; then
            return 1
        fi
    done

    true # not strictly needed
}

# Check if curl supports the --retry flag, then pass it to the curl invocation.
check_curl_for_retry_support() {
  local _retry_supported=""
  # "unspecified" is for arch, allows for possibility old OS using macports, homebrew, etc.
  if check_help_for "notspecified" "curl" "--retry"; then
    _retry_supported="--retry 3"
  fi

  RETVAL="$_retry_supported"

}

# Return cipher suite string specified by user, otherwise return strong TLS 1.2-1.3 cipher suites
# if support by local tools is detected. Detection currently supports these curl backends:
# GnuTLS and OpenSSL (possibly also LibreSSL and BoringSSL). Return value can be empty.
get_ciphersuites_for_curl() {
    if [ -n "${KAMU_TLS_CIPHERSUITES-}" ]; then
        # user specified custom cipher suites, assume they know what they're doing
        RETVAL="$KAMU_TLS_CIPHERSUITES"
        return
    fi

    local _openssl_syntax="no"
    local _gnutls_syntax="no"
    local _backend_supported="yes"
    if curl -V | grep -q ' OpenSSL/'; then
        _openssl_syntax="yes"
    elif curl -V | grep -iq ' LibreSSL/'; then
        _openssl_syntax="yes"
    elif curl -V | grep -iq ' BoringSSL/'; then
        _openssl_syntax="yes"
    elif curl -V | grep -iq ' GnuTLS/'; then
        _gnutls_syntax="yes"
    else
        _backend_supported="no"
    fi

    local _args_supported="no"
    if [ "$_backend_supported" = "yes" ]; then
        # "unspecified" is for arch, allows for possibility old OS using macports, homebrew, etc.
        if check_help_for "notspecified" "curl" "--tlsv1.2" "--ciphers" "--proto"; then
            _args_supported="yes"
        fi
    fi

    local _cs=""
    if [ "$_args_supported" = "yes" ]; then
        if [ "$_openssl_syntax" = "yes" ]; then
            _cs=$(get_strong_ciphersuites_for "openssl")
        elif [ "$_gnutls_syntax" = "yes" ]; then
            _cs=$(get_strong_ciphersuites_for "gnutls")
        fi
    fi

    RETVAL="$_cs"
}

# Return cipher suite string specified by user, otherwise return strong TLS 1.2-1.3 cipher suites
# if support by local tools is detected. Detection currently supports these wget backends:
# GnuTLS and OpenSSL (possibly also LibreSSL and BoringSSL). Return value can be empty.
get_ciphersuites_for_wget() {
    if [ -n "${KAMU_TLS_CIPHERSUITES-}" ]; then
        # user specified custom cipher suites, assume they know what they're doing
        RETVAL="$KAMU_TLS_CIPHERSUITES"
        return
    fi

    local _cs=""
    if wget -V | grep -q '\-DHAVE_LIBSSL'; then
        # "unspecified" is for arch, allows for possibility old OS using macports, homebrew, etc.
        if check_help_for "notspecified" "wget" "TLSv1_2" "--ciphers" "--https-only" "--secure-protocol"; then
            _cs=$(get_strong_ciphersuites_for "openssl")
        fi
    elif wget -V | grep -q '\-DHAVE_LIBGNUTLS'; then
        # "unspecified" is for arch, allows for possibility old OS using macports, homebrew, etc.
        if check_help_for "notspecified" "wget" "TLSv1_2" "--ciphers" "--https-only" "--secure-protocol"; then
            _cs=$(get_strong_ciphersuites_for "gnutls")
        fi
    fi

    RETVAL="$_cs"
}

# Return strong TLS 1.2-1.3 cipher suites in OpenSSL or GnuTLS syntax. TLS 1.2
# excludes non-ECDHE and non-AEAD cipher suites. DHE is excluded due to bad
# DH params often found on servers (see RFC 7919). Sequence matches or is
# similar to Firefox 68 ESR with weak cipher suites disabled via about:config.
# $1 must be openssl or gnutls.
get_strong_ciphersuites_for() {
    if [ "$1" = "openssl" ]; then
        # OpenSSL is forgiving of unknown values, no problems with TLS 1.3 values on versions that don't support it yet.
        echo "TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_256_GCM_SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384"
    elif [ "$1" = "gnutls" ]; then
        # GnuTLS isn't forgiving of unknown values, so this may require a GnuTLS version that supports TLS 1.3 even if wget doesn't.
        # Begin with SECURE128 (and higher) then remove/add to build cipher suites. Produces same 9 cipher suites as OpenSSL but in slightly different order.
        echo "SECURE128:-VERS-SSL3.0:-VERS-TLS1.0:-VERS-TLS1.1:-VERS-DTLS-ALL:-CIPHER-ALL:-MAC-ALL:-KX-ALL:+AEAD:+ECDHE-ECDSA:+ECDHE-RSA:+AES-128-GCM:+CHACHA20-POLY1305:+AES-256-GCM"
    fi
}

#########################################################################################
# SemVer
#########################################################################################

semver_parse() {
  local RE='v?([0-9]+)[.]([0-9]+)[.]([0-9]+)([.0-9A-Za-z-]*)'

  # # strip word "v" if exists
  # version=$(echo "${1//v/}")

  #MAJOR
  eval $2=$(echo $1 | sed -E "s#$RE#\1#")
  #MINOR
  eval $3=$(echo $1 | sed -E "s#$RE#\2#")
  #MINOR
  eval $4=$(echo $1 | sed -E "s#$RE#\3#")
  #SPECIAL
  eval $5=$(echo $1 | sed -E "s#$RE#\4#")
}

# Code inspired (copied partially and improved) with attributions from:
# https://github.com/cloudflare/semver_bash/blob/master/semver.sh
# https://gist.github.com/Ariel-Rodriguez/9e3c2163f4644d7a389759b224bfe7f3
semver_compare() {
  local version_a version_b

  local MAJOR_A=0
  local MINOR_A=0
  local PATCH_A=0
  local SPECIAL_A=0

  local MAJOR_B=0
  local MINOR_B=0
  local PATCH_B=0
  local SPECIAL_B=0

  semver_parse $1 MAJOR_A MINOR_A PATCH_A SPECIAL_A
  semver_parse $2 MAJOR_B MINOR_B PATCH_B SPECIAL_B

  # Check if our version is higher
  if [ $MAJOR_A -gt $MAJOR_B ]; then
    echo 1 && return 0
  fi
  if [ $MAJOR_A -eq $MAJOR_B ]; then
    if [ $MINOR_A -gt $MINOR_B ]; then
      echo 1 && return 0
    elif [ $MINOR_A -eq $MINOR_B ]; then
      if [ $PATCH_A -gt $PATCH_B ]; then
        echo 1 && return 0
      elif [ $PATCH_A -eq $PATCH_B ]; then
        if [ -n "$SPECIAL_A" ] && [ -z "$SPECIAL_B" ]; then
          # if the version we're targeting does not have a tag and our current
          # version does, we should upgrade because no tag > tag
          echo -1 && return 0
        elif [ "$SPECIAL_A" \> "$SPECIAL_B" ]; then
          echo 1 && return 0
        elif [ "$SPECIAL_A" = "$SPECIAL_B" ]; then
          # complete match
          echo 0 && return 0
        fi
      fi
    fi
  fi

  # if we're here we know that the target version cannot be less than or equal to
  # our current version, therefore we upgrade

  echo -1 && return 0
}

#########################################################################################

main "$@" || exit 1
