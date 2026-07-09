#!/usr/bin/env sh
set -eu

if [ -n "${CARGO_HOME:-}" ] && [ -d "${CARGO_HOME}/bin" ]; then
  case ":${PATH:-}:" in
    *":${CARGO_HOME}/bin:"*) ;;
    *) export PATH="${CARGO_HOME}/bin:${PATH:-}" ;;
  esac
elif [ -d /usr/local/cargo/bin ]; then
  case ":${PATH:-}:" in
    *":/usr/local/cargo/bin:"*) ;;
    *) export PATH="/usr/local/cargo/bin:${PATH:-}" ;;
  esac
fi
