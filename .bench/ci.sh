#!/usr/bin/env bash
# B1.5d benchmark check: the identical command in the Forge and GitHub arms.
# Cold Vite+ toolchain (Node + pnpm pinned by package.json), filtered install, Chromium
# headless shell for the server preview tests, then the pre-registered scope: the full test
# suites of five packages plus apps/server/src/preview. Every suite runs; any failure fails.
set -uo pipefail
cd "$(dirname "$0")/.."
start=$(date +%s)
export VP_VERSION=1.4.0 VITE_PLUS_VERSION=1.4.0 CI=true
for attempt in 1 2 3; do
  curl -fsSL --connect-timeout 10 --max-time 120 https://viteplus.dev/install.sh | bash && break
  [ "$attempt" = 3 ] && exit 1; sleep 5
done
export PATH="$HOME/.local/share/vite-plus/bin:$HOME/.vite-plus/bin:$PATH"
[ -f "$HOME/.config/vite-plus/env" ] && . "$HOME/.config/vite-plus/env"
vp env use "$(node -p 'require("./package.json").engines.node' 2>/dev/null || sed -nE 's/.*"node": "([0-9.]+)".*/\1/p' package.json | head -1)" || exit 1
packages="@t3tools/shared @t3tools/provider-pi @t3tools/client-runtime @t3tools/source-control-forgejo effect-codex-app-server"
filters=(--filter "t3...")
for p in $packages; do filters+=(--filter "$p..."); done
vp install "${filters[@]}" || exit 1
# Same browser as upstream's server CI: playwright-core's pinned headless shell.
pw=apps/server/node_modules/playwright-core/cli.js
# ubuntu-latest is 26.04, which playwright-core 1.60 does not list; use its 24.04 build
# (upstream runs these tests on Ubuntu 24.04). Same override in both arms (same runner).
if [ "$(uname)" = Linux ]; then export PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64; fi
for attempt in 1 2 3; do node "$pw" install --only-shell chromium && break
  [ "$attempt" = 3 ] && exit 1; sleep 5; done
browser=$(find "${PLAYWRIGHT_BROWSERS_PATH:-$HOME/.cache/ms-playwright}" "$HOME/Library/Caches/ms-playwright" -name chrome-headless-shell -type f -print -quit 2>/dev/null)
if [ "$(uname)" = Linux ] && ldd "$browser" | grep -q '=> not found'; then node "$pw" install-deps chromium || exit 1; fi
echo "bench-ci: setup $(( $(date +%s) - start ))s"
status=0
for p in $packages; do
  t=$(date +%s)
  vp run --filter "$p" test || { status=1; echo "bench-ci: FAILED $p"; }
  echo "bench-ci: $p $(( $(date +%s) - t ))s"
done
t=$(date +%s)
(cd apps/server && vp test run src/preview) || { status=1; echo "bench-ci: FAILED t3 src/preview"; }
echo "bench-ci: t3 src/preview $(( $(date +%s) - t ))s"
echo "bench-ci: total $(( $(date +%s) - start ))s"
exit $status
