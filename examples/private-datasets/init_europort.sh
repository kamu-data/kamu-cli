#!/usr/bin/env bash

set -euo pipefail

kamu --account alice login https://platform.europort.kamu.dev

kamu --account alice push 1pub1 --to platform.europort.kamu.dev/alice/1pub1 --visibility public --force
kamu --account alice push 1priv1 --to platform.europort.kamu.dev/alice/1priv1 --visibility private --force
kamu --account alice push 2priv --to platform.europort.kamu.dev/alice/2priv --visibility private --force
kamu --account alice push 1priv2 --to platform.europort.kamu.dev/alice/1priv2 --visibility private --force
kamu --account alice push 1pub2 --to platform.europort.kamu.dev/alice/1pub2 --visibility public --force
kamu --account alice push 2pub --to platform.europort.kamu.dev/alice/2pub --visibility public --force
kamu --account alice push 3pub --to platform.europort.kamu.dev/alice/3pub --visibility public --force
kamu --account alice push 4priv --to platform.europort.kamu.dev/alice/4priv --visibility private --force
kamu --account alice push 4pub --to platform.europort.kamu.dev/alice/4pub --visibility public --force
kamu --account alice push 5priv1 --to platform.europort.kamu.dev/alice/5priv1 --visibility private --force
kamu --account alice push 5priv2 --to platform.europort.kamu.dev/alice/5priv2 --visibility private --force
kamu --account alice push 5pub1 --to platform.europort.kamu.dev/alice/5pub1 --visibility public --force
kamu --account alice push 5pub2 --to platform.europort.kamu.dev/alice/5pub2 --visibility public --force
