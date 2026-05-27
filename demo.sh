#!/usr/bin/env bash

set -euo pipefail

SERVER_URL="${SERVER_URL:-http://localhost:8000}"
WORK_DIR="${WORK_DIR:-./demo-output}"
TEST_FILE="${WORK_DIR}/demo-file.txt"
DOWNLOAD_DIR="${WORK_DIR}/downloads"
DOWNLOAD_FILE="${DOWNLOAD_DIR}/demo-file.txt"

mkdir -p "${WORK_DIR}" "${DOWNLOAD_DIR}"

cat > "${TEST_FILE}" <<'EOF'
Campus CDN demo file
This file exercises upload, manifest retrieval, chunk download,
watch party room creation, and analytics summary reporting.
EOF

echo "Waiting for server at ${SERVER_URL}..."
for _ in $(seq 1 30); do
  if curl -fsS "${SERVER_URL}/health" >/dev/null; then
    break
  fi
  sleep 1
done

if ! curl -fsS "${SERVER_URL}/health" >/dev/null; then
  echo "Server is not reachable at ${SERVER_URL}" >&2
  exit 1
fi

echo
echo "Uploading test file..."
UPLOAD_RESPONSE="$(curl -fsS -X POST "${SERVER_URL}/upload" -F "file=@${TEST_FILE}")"
printf '%s\n' "${UPLOAD_RESPONSE}"

FILE_ID="$(
  UPLOAD_RESPONSE="${UPLOAD_RESPONSE}" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["UPLOAD_RESPONSE"])
print(payload["file_id"])
PY
)"

echo
echo "Fetching manifest for ${FILE_ID}..."
MANIFEST_RESPONSE="$(curl -fsS "${SERVER_URL}/manifest/${FILE_ID}")"
printf '%s\n' "${MANIFEST_RESPONSE}"

echo
echo "Downloading chunks back with curl..."
MANIFEST_RESPONSE="${MANIFEST_RESPONSE}" SERVER_URL="${SERVER_URL}" FILE_ID="${FILE_ID}" DOWNLOAD_FILE="${DOWNLOAD_FILE}" python3 - <<'PY'
import json
import os
import pathlib
import subprocess

manifest = json.loads(os.environ["MANIFEST_RESPONSE"])
server_url = os.environ["SERVER_URL"].rstrip("/")
file_id = os.environ["FILE_ID"]
download_file = pathlib.Path(os.environ["DOWNLOAD_FILE"])
download_file.parent.mkdir(parents=True, exist_ok=True)

with download_file.open("wb") as out:
    for chunk in sorted(manifest["chunks"], key=lambda item: item["index"]):
        url = f"{server_url}/chunk/{file_id}/{chunk['index']}"
        print(f"curl {url}")
        data = subprocess.check_output(["curl", "-fsS", url])
        out.write(data)

print(download_file)
PY

echo
echo "Downloaded file contents:"
cat "${DOWNLOAD_FILE}"

echo
echo "Creating watch party room..."
WATCHPARTY_RESPONSE="$(curl -fsS -X POST "${SERVER_URL}/watchparty/create" \
  -H "Content-Type: application/json" \
  -d "{\"host_id\":\"demo-host\",\"file_id\":\"${FILE_ID}\"}")"
printf '%s\n' "${WATCHPARTY_RESPONSE}"

echo
echo "Analytics summary..."
curl -fsS "${SERVER_URL}/analytics/summary"
echo
