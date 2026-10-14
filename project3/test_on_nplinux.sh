#!/usr/bin/env bash
# Stage 1 smoke test on NP Linux Workstation.
# Run from inside ~/np_project3/  (where the source has been copied to).
# Pre-req: on nplinux, run `make clean && make http_server` first.

set -uo pipefail

PORT=${1:-18080}
echo "=== nplinux: $(hostname) ==="
echo "=== arch: $(uname -m) ==="
echo "=== g++ version ==="; g++ --version | head -1
echo "=== boost version ==="
grep "BOOST_VERSION " /usr/include/boost/version.hpp 2>/dev/null \
  || grep "BOOST_VERSION " ~/include/boost/version.hpp 2>/dev/null \
  || echo "(can't locate boost/version.hpp)"

echo
echo "=== build (plain make, no BOOST_ROOT) ==="
make clean >/dev/null 2>&1
if make http_server 2>&1; then
  echo "[ok] build succeeded"
else
  echo "[FAIL] build failed"; exit 1
fi

echo
echo "=== smoke: launching http_server on port $PORT ==="
./http_server "$PORT" &
SVR=$!
trap "kill $SVR 2>/dev/null; wait 2>/dev/null" EXIT
sleep 0.4

if ! kill -0 "$SVR" 2>/dev/null; then
  echo "[FAIL] server died on launch"; exit 1
fi

echo
echo "=== test 1: printenv.cgi (verifies all 9 env vars) ==="
RESPONSE=$(curl -sS -i --max-time 4 "http://127.0.0.1:$PORT/printenv.cgi?foo=bar&baz=qux")
echo "$RESPONSE"
echo
for v in REQUEST_METHOD REQUEST_URI QUERY_STRING SERVER_PROTOCOL HTTP_HOST SERVER_ADDR SERVER_PORT REMOTE_ADDR REMOTE_PORT; do
  if echo "$RESPONSE" | grep -q "^$v = "; then
    echo "  [ok] $v present"
  else
    echo "  [FAIL] $v missing"
  fi
done

echo
echo "=== test 2: panel.cgi (verifies HTML rendering + test_case listing) ==="
PANEL=$(curl -sS --max-time 4 "http://127.0.0.1:$PORT/panel.cgi")
NHOSTS=$(echo "$PANEL" | grep -c '<option value="nplinux')
NTCS=$(echo "$PANEL" | grep -c '<option value="t.\.txt">')
echo "  nplinux* hosts in dropdown: $NHOSTS  (expect 12)"
echo "  test_case files in dropdown: $NTCS  (expect 5)"

echo
echo "=== done ==="
