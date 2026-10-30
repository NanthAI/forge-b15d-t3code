#!/usr/bin/env bash
# B1.5d benchmark check: the identical command in the Forge and GitHub arms.
# Cold Vite+ toolchain (Node + pnpm pinned by package.json), filtered install,
# then the full test suites of the four packages the benchmark tasks touch.
set -euo pipefail
cd "$(dirname "$0")/.."
start=$(date +%s)
export VP_VERSION=1.4.0 VITE_PLUS_VERSION=1.4.0 CI=true
for attempt in 1 2 3; do
  curl -fsSL --connect-timeout 10 --max-time 120 https://viteplus.dev/install.sh | bash && break
  [ "$attempt" = 3 ] && exit 1; sleep 5
done
export PATH="$HOME/.local/share/vite-plus/bin:$HOME/.vite-plus/bin:$PATH"
[ -f "$HOME/.config/vite-plus/env" ] && . "$HOME/.config/vite-plus/env"
vp env use "$(node -p 'require("./package.json").engines.node' 2>/dev/null || sed -nE 's/.*"node": "([0-9.]+)".*/\1/p' package.json | head -1)"
packages="shared provider-pi client-runtime source-control-forgejo"
filters=(); for p in $packages; do filters+=(--filter "@t3tools/$p..."); done
vp install "${filters[@]}"
echo "bench-ci: setup $(( $(date +%s) - start ))s"
for p in $packages; do
  t=$(date +%s)
  vp run --filter "@t3tools/$p" test
  echo "bench-ci: $p $(( $(date +%s) - t ))s"
done
echo "bench-ci: total $(( $(date +%s) - start ))s"
