#!/usr/bin/env bash
# Run from the existing course Ubuntu VM after az login. Requires Azure CLI and AzCopy.
set -euo pipefail
lake="${1:?Usage: bash azure/stage_source.sh STORAGE_ACCOUNT PATH_TO_CSV}"
source_path="${2:?Provide the path to all_stock_data.csv}"
[[ "$lake" =~ ^[a-z0-9]{3,24}$ && -f "$source_path" ]]
[[ "$(basename "$source_path")" == "all_stock_data.csv" ]]
sha="$(sha256sum "$source_path" | cut -d ' ' -f 1)"
relative="stock_phase1/releases/$sha/all_stock_data.csv"
bytes="$(stat -c %s "$source_path")"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="$script_dir/../project.json"

python3 - "$config" "$sha" "$bytes" <<'PY'
import json
import sys
from pathlib import Path

config, digest, byte_count = sys.argv[1:]
release = json.loads(Path(config).read_text())["csv_release"]
if (digest, int(byte_count)) != (release["sha256"], release["bytes"]):
    raise SystemExit("CSV hash or size differs from the pinned source. Upload stopped.")
PY

export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
azcopy copy "$source_path" "https://$lake.blob.core.windows.net/raw/$relative" --overwrite=false

# A server size check plus a downloaded SHA-256 covers both first uploads and skipped copies.
remote="$(mktemp -d --tmpdir="$(dirname "$source_path")")"
trap 'rm -rf -- "$remote"' EXIT
az storage blob download --account-name "$lake" --container-name raw --name "$relative" \
  --auth-mode login --file "$remote/all_stock_data.csv" --only-show-errors >/dev/null
[[ "$(stat -c %s "$remote/all_stock_data.csv")" -eq "$bytes" ]]
printf '%s  %s\n' "$sha" "$remote/all_stock_data.csv" | sha256sum --check --status
python3 - "$config" "$lake" "$relative" "$sha" "$bytes" <<'PY'
import json
import sys
from pathlib import Path

config, lake, path, digest, byte_count = sys.argv[1:]
project = json.loads(Path(config).read_text())
print(json.dumps({"source_url": project["source_url"], "source_release": project["csv_release"]["version"],
                  "format": "csv", "raw_path": f"abfss://raw@{lake}.dfs.core.windows.net/{path}",
                  "sha256": digest, "bytes": int(byte_count), "copy_verification": "sha256_match"}, indent=2))
PY
