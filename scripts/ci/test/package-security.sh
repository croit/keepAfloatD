#!/usr/bin/env sh
set -eu

WORKDIR="${CI_PROJECT_DIR:-$(pwd)}"
cd "${WORKDIR}"

deb_asset='["config.example.yaml", "etc/keepafloatd/config.yaml", "600"]'
rpm_asset='{ source = "config.example.yaml", dest = "/etc/keepafloatd/config.yaml", mode = "600", config = "noreplace", user = "root", group = "root" }'

grep -Fqx "  ${deb_asset}," Cargo.toml || {
  echo "Debian config asset must be installed with mode 600" >&2
  exit 1
}
grep -Fqx "  ${rpm_asset}," Cargo.toml || {
  echo "RPM config asset must be installed with mode 600" >&2
  exit 1
}

grep -Fqx 'post_install_script = "deploy/rpm/postinst"' Cargo.toml || {
  echo "RPM package must harden existing instance configs after upgrades" >&2
  exit 1
}
grep -Fq 'chmod 0600' deploy/debian/postinst || {
  echo "Debian postinst must harden existing instance configs" >&2
  exit 1
}
grep -Fq 'chown root:root' deploy/debian/postinst || {
  echo "Debian postinst must restore root ownership on existing instance configs" >&2
  exit 1
}
grep -Fq 'chmod 0600' deploy/rpm/postinst || {
  echo "RPM postinst must harden existing instance configs" >&2
  exit 1
}
grep -Fq 'chown root:root' deploy/rpm/postinst || {
  echo "RPM postinst must restore root ownership on existing instance configs" >&2
  exit 1
}
