#!/usr/bin/env sh
set -eu

WORKDIR="${CI_PROJECT_DIR:-$(pwd)}"
export DEBIAN_FRONTEND=noninteractive

cd "${WORKDIR}"

apt-get update -qq
apt-get install -y -qq --no-install-recommends "./dist/"*_amd64.deb

/usr/bin/keepafloatd --help >/dev/null
test -f /etc/keepafloatd/config.yaml
test -f /etc/default/keepafloatd

dpkg -s keepafloatd >/dev/null
dpkg -L keepafloatd | grep -q '/systemd/system/keepafloatd@.service'

rm -rf /var/lib/apt/lists/*
