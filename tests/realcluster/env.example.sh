#!/usr/bin/env bash
# Copy to env.sh and replace every example value for the target cluster. env.sh is ignored.

REALCLUSTER_NAME="example-cluster"
MGMT="192.0.2.10"

NODE_IPS=(192.0.2.101 192.0.2.102 192.0.2.103)
NODE_INSTANCES=(node-a node-b node-c)
NODE_RAFT_IDS=(1 2 3)
NODE_SERVERIDS=(1 2 3)
VIPS=(198.51.100.210 198.51.100.211 198.51.100.212)
IFACE="eth0"

# Disposable addresses/interfaces used only by mutation scenarios and removed afterward.
VLAN_TEST_ID=200
VLAN_TEST_VIP="198.51.100.220"
CIDR_TEST_VIP="198.51.100.221"
CIDR_TEST_PREFIX=24
IPV6_TEST_VIP="2001:db8::10"
MUTATION_TEST_VIP="198.51.100.222"
OWNERSHIP_ADMIN_VIP="198.51.100.223"
OWNERSHIP_FOREIGN_VIP="198.51.100.224"
OWNERSHIP_FOREIGN_PROTOCOL=245
PARTITION_PORTS="9210,9211"

# Cluster management API. Leave the password empty in this template and provide it only in env.sh
# or the process environment.
CROIT_URL="https://192.0.2.10"
CROIT_USER="admin"
CROIT_PASS="${CROIT_PASS:-}"

# Exact candidate identity required by the baseline and final audit.
FIXED_BUILDID="replace-with-candidate-build-id"
SSH_COMMAND_TIMEOUT="${SSH_COMMAND_TIMEOUT:-60}"

# D15/D16 require a separately prepared legacy executable and its exact BuildID.
LEGACY_BUILDID="${LEGACY_BUILDID:-}"
LEGACY_BINARY="${LEGACY_BINARY:-}"

S3_ACCESS="${S3_ACCESS:-}"
S3_SECRET="${S3_SECRET:-}"
S3_BUCKET="keepafloatd-test"

SSH_CTL_DIR="${SSH_CTL_DIR:-/tmp/keepafloatd-realcluster-ssh}"
SSH_OPTS_BASE="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10"
SSH_OPTS="${SSH_OPTS_BASE} -o ControlMaster=auto -o ControlPath=${SSH_CTL_DIR}/%r@%h:%p -o ControlPersist=300"
NODE_SSH_OPTS="${SSH_OPTS_BASE} -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null"
NODE_KEY="~/.ssh/id_ed25519"
