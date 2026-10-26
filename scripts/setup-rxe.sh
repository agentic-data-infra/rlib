#!/usr/bin/env bash
# Sets up Soft-RoCE (the rdma_rxe kernel driver) so rlib's tests and examples
# run on a machine without an RDMA NIC: installs rdma-core and its tools,
# loads rdma_rxe and adds an rxe device on top of a network interface.
# Idempotent; run it again after a reboot (the device does not persist).
#
# Usage: scripts/setup-rxe.sh [NETDEV]   (default: the default route's)
set -euo pipefail

netdev=${1:-$(ip -o route show default | awk '{print $5; exit}')}
rxe=rxe0

packages=(rdma-core ibverbs-utils rdmacm-utils libibverbs-dev librdmacm-dev
  perftest)
# Cloud kernels (e.g. GCP's linux-gcp) ship rdma_rxe in the extra modules.
if ! modinfo rdma_rxe >/dev/null 2>&1; then
  packages+=("linux-modules-extra-$(uname -r)")
fi
missing=()
for p in "${packages[@]}"; do
  dpkg -s "$p" >/dev/null 2>&1 || missing+=("$p")
done
if ((${#missing[@]})); then
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "${missing[@]}"
fi

sudo modprobe rdma_rxe
if ! rdma link show | grep -q "netdev $netdev\b"; then
  sudo rdma link add "$rxe" type rxe netdev "$netdev"
fi
rdma link show
ibv_devices

# Memory registration pins pages, up to RLIMIT_MEMLOCK.
memlock=$(ulimit -l)
if [[ $memlock != unlimited ]] && ((memlock < 1048576)); then
  echo "Note: the locked memory limit is ${memlock} KiB, which limits how much"
  echo "memory can be registered. Raise it, e.g. in /etc/security/limits.conf:"
  echo "  $USER - memlock unlimited"
fi

addr=$(ip -o -4 addr show "$netdev" | awk '{print $4; exit}')
echo "Soft-RoCE is up on $netdev (${addr%/*}). Test it with rping against that"
echo "address (not 127.0.0.1):"
echo "  rping -s -a ${addr%/*} -C 3 &  rping -c -a ${addr%/*} -C 3 -v"
