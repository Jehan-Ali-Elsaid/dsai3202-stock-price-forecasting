#!/usr/bin/env bash
# Verify the ADF output, then stage a byte-identical copy in the course Lab 3 lake.
set -euo pipefail

project_lake="${1:?Usage: bash azure/stage_databricks_input.sh PROJECT_LAKE LAB_LAKE}"
lab_lake="${2:?Provide the Lab 3 storage account name}"
[[ "$project_lake" =~ ^[a-z0-9]{3,24}$ && "$lab_lake" =~ ^[a-z0-9]{3,24}$ && "$project_lake" != "$lab_lake" ]]
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p local-data evidence/azure
tmp="$(mktemp -d --tmpdir="$PWD/local-data")"
trap 'rm -rf -- "$tmp"' EXIT

release_sha="$(python3 -c 'import json; print(json.load(open("project.json"))["csv_release"]["sha256"])')"
raw_blob="stock_phase1/releases/$release_sha/all_stock_data.csv"
bronze_blob="stock_phase1/bronze_csv/$release_sha/all_stock_data.parquet"
stage_folder="stock_phase1/handoff/$release_sha"
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI

blob_show() {
    az storage blob show --account-name "$1" --container-name "$2" --name "$3" \
        --auth-mode login --only-show-errors -o json
}

download() {
    az storage blob download --account-name "$1" --container-name "$2" --name "$3" \
        --auth-mode login --only-show-errors --file "$4" -o none
}

printf '%s\n' 'Checking the original raw CSV and the ADF Parquet.'
az storage blob list --account-name "$project_lake" --container-name raw \
    --prefix "${raw_blob%/*}/" --auth-mode login --only-show-errors \
    --query '[].{name:name,etag:properties.etag,modified:properties.lastModified,bytes:properties.contentLength}' \
    -o json > "$tmp/raw.json"
blob_show "$project_lake" processed "$bronze_blob" > "$tmp/source_before.json"

python3 - "$tmp/raw.json" "$project_lake" "$raw_blob" <<'PY'
import json
import sys
from datetime import datetime
from pathlib import Path

metadata_file, lake, blob = sys.argv[1:]
config = json.loads(Path("project.json").read_text())
release = config["csv_release"]
evidence = Path("evidence/azure")
before = json.loads((evidence / "raw_before_rerun.json").read_text())
after = json.loads((evidence / "raw_after_rerun.json").read_text())
current = json.loads(Path(metadata_file).read_text())

def raw_identity(snapshot):
    if len(snapshot) != 1:
        raise SystemExit("Expected exactly one CSV in the raw release folder.")
    item = snapshot[0]
    if not item["etag"] or not item["modified"]:
        raise SystemExit("The raw snapshot is missing its ETag or last-modified time.")
    modified = datetime.fromisoformat(item["modified"].replace("Z", "+00:00"))
    if modified.utcoffset() is None:
        raise SystemExit("The raw snapshot timestamp must include a timezone.")
    return item["name"], int(item["bytes"]), item["etag"].strip('"'), modified

prior, repeated, observed = map(raw_identity, (before, after, current))
if prior != repeated:
    raise SystemExit("The raw before and after snapshots differ.")
fields = ("name", "bytes", "ETag", "last-modified time")
changed = [name for name, earlier, now in zip(fields, repeated, observed) if earlier != now]
if changed:
    raise SystemExit(f"The current raw blob differs from the verified rerun in: {', '.join(changed)}.")
if observed[:2] != (blob, release["bytes"]):
    raise SystemExit("The raw name or size differs from the pinned release.")
raw_path = f"abfss://raw@{lake}.dfs.core.windows.net/{blob}"
for line in (evidence / "raw_upload_rerun.log").read_text().splitlines():
    if '"copy_verification": "sha256_match"' in line:
        break
else:
    raise SystemExit("The raw rerun log must include sha256_match.")
log = (evidence / "raw_upload_rerun.log").read_text()
if raw_path not in log or release["sha256"] not in log:
    raise SystemExit("The raw rerun receipt belongs to a different source.")
activity = json.loads((evidence / "adf_csv_to_parquet_output.json").read_text())
if (len(activity) != 1 or activity[0]["status"] != "Succeeded"
        or activity[0]["rowsRead"] != release["rows"]
        or activity[0]["rowsCopied"] != release["rows"] or activity[0]["filesWritten"] != 1):
    raise SystemExit("The ADF evidence must show a successful full CSV-to-Parquet conversion.")
PY

printf '%s\n' 'Copying the Parquet to the Lab 3 processed container.'
azcopy copy "https://$project_lake.blob.core.windows.net/processed/$bronze_blob" \
    "https://$lab_lake.blob.core.windows.net/processed/$bronze_blob" --overwrite=false

printf '%s\n' 'Verifying both Parquet copies with SHA-256.'
download "$project_lake" processed "$bronze_blob" "$tmp/parquet"
source_sha="$(sha256sum "$tmp/parquet" | cut -d ' ' -f 1)"
rm -- "$tmp/parquet"
download "$lab_lake" processed "$bronze_blob" "$tmp/parquet"
bronze_sha="$(sha256sum "$tmp/parquet" | cut -d ' ' -f 1)"
[[ "$source_sha" == "$bronze_sha" ]] || { printf '%s\n' 'Parquet SHA-256 mismatch.' >&2; exit 1; }
blob_show "$project_lake" processed "$bronze_blob" > "$tmp/source_after.json"
blob_show "$lab_lake" processed "$bronze_blob" > "$tmp/lab_bronze.json"

put_verified() {
    azcopy copy "$1" "https://$lab_lake.blob.core.windows.net/processed/$2" --overwrite=false
    rm -f -- "$tmp/small"
    download "$lab_lake" processed "$2" "$tmp/small"
    cmp -s -- "$1" "$tmp/small" || { printf 'Saved file differs: %s\n' "$2" >&2; exit 1; }
}

config_sha="$(sha256sum project.json | cut -d ' ' -f 1)"
fixture_sha="$(sha256sum fixtures/quality_test.parquet | cut -d ' ' -f 1)"
config_blob="$stage_folder/project-$config_sha.json"
fixture_blob="$stage_folder/quality_test-$fixture_sha.parquet"
put_verified project.json "$config_blob"
put_verified fixtures/quality_test.parquet "$fixture_blob"
blob_show "$lab_lake" processed "$fixture_blob" > "$tmp/fixture.json"

python3 - "$tmp" "$project_lake" "$lab_lake" "$raw_blob" "$bronze_blob" "$bronze_sha" "$fixture_blob" "$fixture_sha" "$config_blob" <<'PY'
import json
import sys
from datetime import datetime
from pathlib import Path

temp, project, lab, raw_blob, bronze_blob, bronze_sha, fixture_blob, fixture_sha, config_blob = sys.argv[1:]
temp = Path(temp)
config = json.loads(Path("project.json").read_text())
release = config["csv_release"]
evidence = Path("evidence/azure")
adf_run = (evidence / "adf_csv_to_parquet_run_id.txt").read_text().strip()
if not adf_run:
    raise SystemExit("The successful ADF run ID is missing.")
before = json.loads((temp / "source_before.json").read_text())["properties"]
after = json.loads((temp / "source_after.json").read_text())["properties"]
if before["etag"] != after["etag"]:
    raise SystemExit("The original ADF output changed during verification.")
def artifact(metadata, blob, digest):
    props = json.loads((temp / metadata).read_text())["properties"]
    published = datetime.fromisoformat(props["lastModified"].replace("Z", "+00:00"))
    return {"path": f"abfss://processed@{lab}.dfs.core.windows.net/{blob}",
            "bytes": props["contentLength"], "sha256": digest, "etag": props["etag"],
            "modified_epoch": int(published.timestamp())}

bronze = artifact("lab_bronze.json", bronze_blob, bronze_sha)
if bronze["bytes"] != after["contentLength"] or bronze["bytes"] != (temp / "parquet").stat().st_size:
    raise SystemExit("The Parquet copies have different sizes.")
bronze["adf_output_path"] = f"abfss://processed@{project}.dfs.core.windows.net/{bronze_blob}"
bronze["adf_output_etag"] = after["etag"]
fixture = artifact("fixture.json", fixture_blob, fixture_sha)
raw = json.loads((temp / "raw.json").read_text())[0]
receipt = {
    "raw": {"path": f"abfss://raw@{project}.dfs.core.windows.net/{raw_blob}",
            "source_url": config["source_url"], "source_release": release["version"],
            "sha256": release["sha256"], "bytes": release["bytes"], "etag": raw["etag"]},
    "adf_run_id": adf_run, "source_rows": release["rows"], "bronze": bronze, "quality_test": fixture,
    "verification": "Raw SHA-256 receipt and unchanged Azure metadata, plus matching Parquet SHA-256 across accounts."
}
(evidence / "databricks_lab_handoff.json").write_text(json.dumps(receipt, indent=2) + "\n")
(evidence / "databricks_lab_config_path.txt").write_text(
    f"abfss://processed@{lab}.dfs.core.windows.net/{config_blob}\n")
PY

handoff_sha="$(sha256sum evidence/azure/databricks_lab_handoff.json | cut -d ' ' -f 1)"
handoff_blob="$stage_folder/handoff-$handoff_sha.json"
put_verified evidence/azure/databricks_lab_handoff.json "$handoff_blob"
printf 'abfss://processed@%s.dfs.core.windows.net/%s\n' "$lab_lake" "$handoff_blob" \
    > evidence/azure/databricks_lab_handoff_path.txt

python3 - "$lab_lake" <<'PY'
import json
import sys
from pathlib import Path

lab = sys.argv[1]
evidence = Path("evidence/azure")
receipt = json.loads((evidence / "databricks_lab_handoff.json").read_text())
parameters = {
    "config_path": (evidence / "databricks_lab_config_path.txt").read_text().strip(),
    "handoff_path": (evidence / "databricks_lab_handoff_path.txt").read_text().strip(),
    "landing_path": receipt["bronze"]["path"],
    "processed_root": f"abfss://processed@{lab}.dfs.core.windows.net/stock_phase1/silver",
    "root_path": f"abfss://curated@{lab}.dfs.core.windows.net/stock_phase1/project_data",
    "scenario": "full", "code_version": "REPLACE_WITH_GIT_COMMIT", "clean_format": "delta",
    "adf_run_id": receipt["adf_run_id"]
}
settings = {"job_parameters": parameters, "quality_test_landing_path": receipt["quality_test"]["path"]}
(evidence / "databricks_lab_parameters.json").write_text(json.dumps(settings, indent=2) + "\n")
print(json.dumps(settings, indent=2))
print("LAB_HANDOFF_READY")
PY
