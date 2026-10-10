#!/bin/sh
# Print the action.yml for a release: where Probe downloads each executable
# from, and the SHA-256 digest it must have.
#
#   scripts/action-yml.sh <tag> <checksums.txt>
set -eu

tag=$1
checksums=$2
repo=${GITHUB_REPOSITORY:-mozership/probe-websocket}

cat <<YAML
name: websocket
description: Exchange messages with a WebSocket server
guard: [read-only, allow-host]
params: [url, headers, subprotocols, messages, timeout]
runs:
  using: binary
  url: https://github.com/${repo}/releases/download/${tag}/probe-websocket_{os}_{arch}
  checksums:
YAML
# A line of checksums.txt is "<digest>  probe-websocket_<os>_<arch>".
awk '$2 ~ /^probe-websocket_/ { sub(/^probe-websocket_/, "", $2); printf "    %s: \"%s\"\n", $2, $1 }' "$checksums" | sort
