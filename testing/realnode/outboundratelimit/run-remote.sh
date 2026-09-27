#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run the outbound rate-limit acceptance on one existing Linux node without
touching its installed x-ui, Xray, configuration, database, or listeners.

Default mode is read-only plan/preflight. Upload and execution require
--execute. Both binaries run from a random /tmp directory and are removed on
exit. The acceptance process uses loopback-only random ports.

Usage:
  run-remote.sh --host HOST --user USER --key PATH [options]

Required:
  --host HOST            Existing node hostname or IP.
  --user USER            SSH user.
  --key PATH             Existing SSH private key.

Options:
  --port PORT            SSH port (default: 22).
  --socks5 HOST:PORT     Loopback SOCKS5 endpoint for SSH, for example
                         127.0.0.1:39081. Non-loopback proxies are rejected.
  --execute              Upload and run the exact current checkout.
  --output PATH          Also save the JSON acceptance output locally.
  -h, --help             Show this help.
EOF
}

host=""
ssh_user=""
ssh_key=""
ssh_port=22
socks5=""
execute=0
output_path=""

while (($# > 0)); do
  case "$1" in
    --host)
      host=${2:-}
      shift 2
      ;;
    --user)
      ssh_user=${2:-}
      shift 2
      ;;
    --key)
      ssh_key=${2:-}
      shift 2
      ;;
    --port)
      ssh_port=${2:-}
      shift 2
      ;;
    --socks5)
      socks5=${2:-}
      shift 2
      ;;
    --execute)
      execute=1
      shift
      ;;
    --output)
      output_path=${2:-}
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$host" || -z "$ssh_user" || -z "$ssh_key" ]]; then
  echo "--host, --user, and --key are required" >&2
  exit 2
fi
if [[ ! "$host" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "invalid host" >&2
  exit 2
fi
if [[ ! "$ssh_user" =~ ^[A-Za-z_][A-Za-z0-9._-]*$ ]]; then
  echo "invalid SSH user" >&2
  exit 2
fi
if [[ ! "$ssh_port" =~ ^[0-9]+$ ]] || ((ssh_port < 1 || ssh_port > 65535)); then
  echo "invalid SSH port" >&2
  exit 2
fi
if [[ ! -r "$ssh_key" ]]; then
  echo "SSH key is not readable: $ssh_key" >&2
  exit 2
fi
if [[ -n "$socks5" && ! "$socks5" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
  echo "--socks5 accepts only 127.0.0.1:PORT" >&2
  exit 2
fi
if [[ -n "$socks5" ]]; then
  socks5_port=${socks5##*:}
  if ((socks5_port < 1 || socks5_port > 65535)); then
    echo "invalid SOCKS5 port" >&2
    exit 2
  fi
fi
if [[ -n "$output_path" && "$execute" -ne 1 ]]; then
  echo "--output requires --execute" >&2
  exit 2
fi
if [[ -n "$output_path" && -e "$output_path" ]]; then
  echo "refusing to overwrite evidence output: $output_path" >&2
  exit 2
fi

repo_root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
commit_sha=$(git -C "$repo_root" rev-parse HEAD)
checkout_state=clean
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  checkout_state=dirty
fi
if [[ "$execute" -eq 1 && "$checkout_state" != "clean" ]]; then
  echo "refusing to build an uncommitted checkout: $repo_root" >&2
  exit 3
fi

ssh_target="${ssh_user}@${host}"
ssh_options=(
  -i "$ssh_key"
  -p "$ssh_port"
  -o BatchMode=yes
  -o ConnectTimeout=8
  -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes
)
scp_options=(
  -i "$ssh_key"
  -P "$ssh_port"
  -o BatchMode=yes
  -o ConnectTimeout=8
  -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes
)
if [[ -n "$socks5" ]]; then
  proxy_command="nc -x ${socks5} -X 5 %h %p"
  ssh_options+=(-o "ProxyCommand=$proxy_command")
  scp_options+=(-o "ProxyCommand=$proxy_command")
fi

if ! preflight=$(
  ssh "${ssh_options[@]}" "$ssh_target" '
    set -eu
    kernel=$(uname -s)
    arch=$(uname -m)
    free_kb=$(df -Pk /tmp | awk "NR==2 {print \$4}")
    xray_count=$(ps -eo comm= | awk "/^(xray|xray-core|x-ui)$/ {count++} END {print count+0}")
    printf "kernel=%s\narch=%s\nfree_tmp_kb=%s\nexisting_xray_or_xui=%s\n" \
      "$kernel" "$arch" "$free_kb" "$xray_count"
  ' 2>&1
); then
  echo "SSH preflight failed for $ssh_target:$ssh_port" >&2
  printf '%s\n' "$preflight" >&2
  exit 6
fi

kernel=$(printf '%s\n' "$preflight" | sed -n 's/^kernel=//p')
remote_arch=$(printf '%s\n' "$preflight" | sed -n 's/^arch=//p')
free_tmp_kb=$(printf '%s\n' "$preflight" | sed -n 's/^free_tmp_kb=//p')
existing_count=$(printf '%s\n' "$preflight" | sed -n 's/^existing_xray_or_xui=//p')
if [[ "$kernel" != "Linux" ]]; then
  echo "target is not Linux: $kernel" >&2
  exit 4
fi
case "$remote_arch" in
  x86_64)
    go_arch=amd64
    ;;
  aarch64|arm64)
    go_arch=arm64
    ;;
  *)
    echo "unsupported remote architecture: $remote_arch" >&2
    exit 4
    ;;
esac
if [[ ! "$free_tmp_kb" =~ ^[0-9]+$ ]] || ((free_tmp_kb < 150000)); then
  echo "target /tmp has less than 150 MB free" >&2
  exit 4
fi

cat <<EOF
target:             $ssh_target:$ssh_port
source commit:      $commit_sha
checkout state:     $checkout_state
remote platform:    linux/$go_arch
remote /tmp free:   $free_tmp_kb KB
existing processes: $existing_count matching xray/xray-core/x-ui
impact:             random /tmp directory; loopback-only random ports
existing services:  not restarted, reconfigured, signalled, or inspected for secrets
mode:               $([[ "$execute" -eq 1 ]] && echo execute || echo plan-only)
EOF

if [[ "$execute" -ne 1 ]]; then
  echo "plan complete; pass --execute to upload and run"
  exit 0
fi

local_tmp=$(mktemp -d "${TMPDIR:-/tmp}/xray-rate-remote.XXXXXX")
remote_tmp=""
cleanup() {
  if [[ -n "$remote_tmp" && "$remote_tmp" == /tmp/xray-rate-acceptance.* ]]; then
    # remote_tmp is accepted only after the strict mktemp-path regex below.
    # shellcheck disable=SC2029
    ssh "${ssh_options[@]}" "$ssh_target" \
      "case '$remote_tmp' in /tmp/xray-rate-acceptance.*) rm -rf -- '$remote_tmp';; esac" \
      >/dev/null 2>&1 || true
  fi
  if [[ "$local_tmp" == "${TMPDIR:-/tmp}"/xray-rate-remote.* ]]; then
    rm -rf -- "$local_tmp"
  fi
}
trap cleanup EXIT INT TERM

echo "building linux/$go_arch binaries from $commit_sha"
(
  cd "$repo_root"
  GOOS=linux GOARCH="$go_arch" CGO_ENABLED=0 \
    go build -trimpath -o "$local_tmp/xray" ./main
  GOOS=linux GOARCH="$go_arch" CGO_ENABLED=0 \
    go build -trimpath -o "$local_tmp/acceptance" ./testing/realnode/outboundratelimit
)
local_xray_hash=$(shasum -a 256 "$local_tmp/xray" | awk '{print $1}')
local_acceptance_hash=$(shasum -a 256 "$local_tmp/acceptance" | awk '{print $1}')

remote_tmp=$(ssh "${ssh_options[@]}" "$ssh_target" \
  'mktemp -d /tmp/xray-rate-acceptance.XXXXXX')
if [[ ! "$remote_tmp" =~ ^/tmp/xray-rate-acceptance\.[A-Za-z0-9]+$ ]]; then
  echo "remote mktemp returned an unsafe path" >&2
  exit 5
fi

scp "${scp_options[@]}" "$local_tmp/xray" "$local_tmp/acceptance" \
  "$ssh_target:$remote_tmp/"
remote_hashes=$(
  # remote_tmp is accepted only after the strict mktemp-path regex above.
  # shellcheck disable=SC2029
  ssh "${ssh_options[@]}" "$ssh_target" \
    "sha256sum '$remote_tmp/xray' '$remote_tmp/acceptance'"
)
remote_xray_hash=$(printf '%s\n' "$remote_hashes" | awk 'NR==1 {print $1}')
remote_acceptance_hash=$(printf '%s\n' "$remote_hashes" | awk 'NR==2 {print $1}')
if [[ "$remote_xray_hash" != "$local_xray_hash" ||
  "$remote_acceptance_hash" != "$local_acceptance_hash" ]]; then
  echo "remote SHA-256 verification failed" >&2
  exit 5
fi

echo "verified uploads: xray=$local_xray_hash acceptance=$local_acceptance_hash"
result=$(
  # remote_tmp is accepted only after the strict mktemp-path regex above.
  # shellcheck disable=SC2029
  ssh "${ssh_options[@]}" "$ssh_target" \
    "'$remote_tmp/acceptance' -xray-bin '$remote_tmp/xray'"
)
printf '%s\n' "$result"
if [[ -n "$output_path" ]]; then
  printf '%s\n' "$result" >"$output_path"
  echo "saved evidence: $output_path"
fi
