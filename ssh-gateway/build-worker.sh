#!/bin/sh
set -eu
cd "$(dirname "$0")"
out=../.wrangler/ssh-wasm
mkdir -p "$out"
GOOS=js GOARCH=wasm go build -trimpath -ldflags='-s -w' -o "$out/gateway.wasm" .
install -m 0644 "$(go env GOROOT)/lib/wasm/wasm_exec.js" "$out/wasm_exec.js"
