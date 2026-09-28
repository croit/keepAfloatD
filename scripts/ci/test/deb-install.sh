#!/usr/bin/env sh
set -eu

WORKDIR="${CI_PROJECT_DIR:-$(pwd)}"
export DEBIAN_FRONTEND=noninteractive

cd "${WORKDIR}"

apt-get update -qq
apt-get install -y -qq --no-install-recommends "./dist/"*_amd64.deb

/usr/bin/keepafloatd --help >/dev/null
test -f /etc/keepafloatd/config.yaml
test "$(stat -c %a /etc/keepafloatd/config.yaml)" = "600"
test "$(stat -c %U:%G /etc/keepafloatd/config.yaml)" = "root:root"
test -f /etc/default/keepafloatd

# A package upgrade must preserve administrator content and repair permissive legacy modes.
printf '\n# administrator-upgrade-marker\n' >>/etc/keepafloatd/config.yaml
chmod 0644 /etc/keepafloatd/config.yaml
dpkg -i ./dist/*_amd64.deb
grep -Fq '# administrator-upgrade-marker' /etc/keepafloatd/config.yaml
test "$(stat -c %a /etc/keepafloatd/config.yaml)" = "600"
test "$(stat -c %U:%G /etc/keepafloatd/config.yaml)" = "root:root"

dpkg -s keepafloatd >/dev/null
dpkg -L keepafloatd | grep -q '/systemd/system/keepafloatd@.service'

rm -rf /var/lib/apt/lists/*
