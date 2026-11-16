# Databricks notebook source
"""DSAI3202 Phase 1. Run as a Databricks notebook or with local PySpark."""

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
from time import perf_counter
from uuid import uuid4

from pyspark.sql import SparkSession, Window, functions as F, types as T
from pyspark import StorageLevel


CHUNK = 8 * 1024 * 1024
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
KINDS = {
    "string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
    "double": T.DoubleType(), "date": T.DateType(), "timestamp": T.TimestampType(),
    "boolean": T.BooleanType(),
}
NUMERIC = {"int", "long", "double"}
FEATURES = {"identity": 1, "log1p": 1, "year": 1, "month": 1, "missing_flag": 1, "ratio": 2}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class LabPath:
    """A governed ADLS URL accessed with the existing Databricks lab identity."""

    def __init__(self, uri):
        self.uri = uri.rstrip("/")

    def __str__(self):
        return self.uri

    def __truediv__(self, name):
        return LabPath(f"{self.uri}/{name}")

    @property
    def parent(self):
        return LabPath(self.uri.rsplit("/", 1)[0])

    @property
    def name(self):
        return self.uri.rsplit("/", 1)[-1]

    def entry(self):
        try:
            files = dbutils.fs.ls(str(self.parent))
        except Exception as error:
            if any(code in str(error) for code in ("FileNotFoundException", "PathNotFound", "[PATH_NOT_FOUND]")):
                return None
            raise
        return next((entry for entry in files if entry.name.rstrip("/") == self.name), None)

    def exists(self):
        return self.entry() is not None

    def is_dir(self):
        entry = self.entry()
        return entry is not None and entry.name.endswith("/")

    def mkdir(self, parents=True, exist_ok=True):
        if not exist_ok and self.exists():
            raise FileExistsError(self.uri)
        if not dbutils.fs.mkdirs(self.uri):
            raise OSError(f"Could not create {self.uri}")


def project_path(value):
    return LabPath(str(value)) if str(value).startswith("abfss://") else Path(value).absolute()


def read_json(path):
    if isinstance(path, LabPath):
        return json.loads(dbutils.fs.head(str(path), 4 * 1024 * 1024))
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(path, LabPath):
        document = json.dumps(value, indent=2, sort_keys=True, allow_nan=False, default=str) + "\n"
        if not dbutils.fs.put(str(path), document, False):
            raise OSError(f"Could not save {path}")
        return
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False, default=str)
        stream.write("\n")


def file_hash(path):
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK), b""):
            digest.update(block)
            byte_count += len(block)
    return digest.hexdigest(), byte_count


def unpacked_size(path, source_format, byte_count):
    if path.name.lower().endswith(".gz"):
        with gzip.open(path, "rb") as stream:
            return sum(len(block) for block in iter(lambda: stream.read(CHUNK), b""))
    if source_format == "parquet":
        from pyarrow import parquet

        with parquet.ParquetFile(path) as source:
            metadata = source.metadata
            return sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
    return byte_count


def ingest(landing, root, config, preserved_source=None):
    format_name = config["format"]
    suffixes = {"csv": (".csv", ".csv.gz"), "json": (".json", ".jsonl", ".ndjson",
                 ".json.gz", ".jsonl.gz", ".ndjson.gz"), "parquet": (".parquet",)}
    paths = sorted(landing.iterdir()) if landing.is_dir() else [landing]
    if not paths or any(not p.is_file() or not p.name.lower().endswith(suffixes[format_name]) for p in paths):
        raise ValueError("Supply one supported data file or a flat folder containing only matching data files.")
    records = []
    for path in paths:
        sha, size = file_hash(path)
        if size == 0:
            raise ValueError(f"Empty landing file: {path}")
        records.append({"name": path.name, "sha256": sha, "bytes": size})
    identity = {"source_url": config["source_url"], "format": format_name, "files": records}
    version = hashlib.sha256(json_bytes(identity)).hexdigest()
    raw = root / "raw" / version
    receipt = raw / "manifest.json"
    raw_paths = [raw / "original" / path.name for path in paths]
    if preserved_source is not None:
        if len(paths) != 1 or paths[0].name != preserved_source.name or paths[0] == preserved_source:
            raise ValueError("Use the matching original source file and a separate ADF copy.")
        raw_paths = [preserved_source]
    if receipt.exists():
        saved = read_json(receipt)
        saved_files = [{k: row[k] for k in ("name", "sha256", "bytes")} for row in saved["files"]]
        if (saved["input_version"] != version or saved["source_url"] != identity["source_url"]
                or saved_files != records or saved["raw_paths"] != list(map(str, raw_paths))):
            raise RuntimeError(f"Receipt mismatch: {receipt}")
        for path, record in zip(raw_paths, records):
            if file_hash(path) != (record["sha256"], record["bytes"]):
                raise RuntimeError(f"Immutable raw file differs from its manifest: {path}")
        return version, receipt, paths if preserved_source else raw_paths, "reused"

    for path, raw_path, record in zip(paths, raw_paths, records):
        expected = (record["sha256"], record["bytes"])
        if preserved_source is None and not raw_path.exists():
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("rb") as source, raw_path.open("xb") as destination:
                shutil.copyfileobj(source, destination, length=CHUNK)
        if file_hash(raw_path) != expected:
            raise RuntimeError(f"Raw copy verification failed: {raw_path}")
        record["uncompressed_bytes"] = unpacked_size(raw_path, format_name, record["bytes"])
    sizes = [record["uncompressed_bytes"] for record in records]
    source_total = sum(sizes) if all(size is not None for size in sizes) else None
    write_json(receipt, {**identity, "input_version": version, "received_utc": timestamp(),
                         "landing_path_at_ingestion": str(landing), "raw_paths": list(map(str, raw_paths)),
                         "total_source_bytes": sum(record["bytes"] for record in records),
                         "total_uncompressed_bytes": source_total,
                         "uncompressed_size_method": "sum_of_parquet_row_group_total_byte_size"
                         if format_name == "parquet" else "uncompressed_file_bytes"})
    return version, receipt, paths if preserved_source else raw_paths, "created"


def ingest_csv_release(landing, root, config, preserved_source):
    from pyarrow import parquet

    release = config["csv_release"]
    if preserved_source is None or preserved_source.name != release["file_name"]:
        raise ValueError("Provide the preserved raw CSV file in preserved_source_path.")
    if landing.suffix != ".parquet" or not landing.is_file():
        raise ValueError("Use the versioned Parquet file created by the ADF conversion as landing_path.")
    source_sha, source_bytes = file_hash(preserved_source)
    if (source_sha, source_bytes) != (release["sha256"], release["bytes"]):
        raise RuntimeError("The raw CSV differs from the pinned Kaggle release.")
    bronze_sha, bronze_bytes = file_hash(landing)
    if not bronze_bytes:
        raise RuntimeError("The converted Parquet file is empty.")
    with parquet.ParquetFile(landing) as converted:
        if converted.metadata.num_rows != release["rows"]:
            raise RuntimeError("The Parquet footer row count differs from the raw CSV release.")
        if set(converted.schema_arrow.names) != {column["source"] for column in config["columns"]}:
            raise RuntimeError("The converted Parquet columns differ from the configured source mapping.")
    identity = {
        "format": "csv", "source_url": config["source_url"], "source_release": release["version"],
        "files": [{"name": release["file_name"], "sha256": source_sha, "bytes": source_bytes}],
    }
    version = hashlib.sha256(json_bytes(identity)).hexdigest()
    receipt = root / "raw" / version / "manifest.json"
    manifest = {
        **identity, "input_version": version, "raw_paths": [str(preserved_source)],
        "download_url": release["download_url"], "expected_source_rows": release["rows"],
        "total_source_bytes": source_bytes, "total_uncompressed_bytes": source_bytes,
        "uncompressed_size_method": "original_uncompressed_csv_bytes",
        "bronze_parquet": {"path": str(landing), "sha256": bronze_sha, "bytes": bronze_bytes},
    }
    if receipt.exists():
        saved = read_json(receipt)
        if any(saved.get(key) != value for key, value in manifest.items()):
            raise RuntimeError("Raw or bronze file differs from the stored release receipt.")
        return version, receipt, [landing], "reused"
    write_json(receipt, {**manifest, "received_utc": timestamp()})
    return version, receipt, [landing], "created"


def ingest_lab_release(landing, root, config, handoff_path, report):
    """Bind the Databricks input to the byte-verified handoff prepared on the VM."""
    if not handoff_path:
        raise ValueError("Provide the VM-verified handoff receipt path.")
    handoff = read_json(project_path(handoff_path))
    full = report["scenario"] == "full"
    artifact = handoff["bronze"] if full else handoff["quality_test"]
    entry = landing.entry()
    if (str(landing) != artifact["path"] or entry is None or entry.name.endswith("/")
            or entry.size != artifact["bytes"] or entry.modificationTime // 1000 != artifact["modified_epoch"]):
        raise RuntimeError("The staged input no longer matches the VM-verified handoff receipt.")

    if full:
        release = config["csv_release"]
        raw = handoff["raw"]
        if (raw["sha256"], raw["bytes"], raw["source_release"], raw["source_url"]) != (
                release["sha256"], release["bytes"], release["version"], config["source_url"]):
            raise RuntimeError("The handoff source does not match the pinned CSV release.")
        if handoff["source_rows"] != release["rows"] or handoff["adf_run_id"] != report["adf_run_id"]:
            raise RuntimeError("The handoff does not identify the verified ADF conversion.")
        identity = {"format": "csv", "source_url": config["source_url"], "source_release": release["version"],
                    "files": [{"name": release["file_name"], "sha256": raw["sha256"], "bytes": raw["bytes"]}]}
        lineage = {"raw_paths": [raw["path"]], "expected_source_rows": release["rows"],
                   "total_uncompressed_bytes": raw["bytes"], "bronze_parquet": artifact}
    else:
        if report["scenario"] != "quality_test":
            raise ValueError("The lab handoff supports full and quality_test scenarios.")
        identity = {"format": "parquet", "source_url": "urn:dsai3202:synthetic-stock-quality-fixture",
                    "files": [{"name": landing.name, "sha256": artifact["sha256"], "bytes": artifact["bytes"]}]}
        lineage = {"raw_paths": [str(landing)]}
    version = hashlib.sha256(json_bytes(identity)).hexdigest()
    receipt = root / "raw" / version / "manifest.json"
    manifest = {**identity, **lineage, "input_version": version, "handoff_receipt": str(handoff_path)}
    if receipt.exists():
        saved = read_json(receipt)
        if any(saved.get(key) != value for key, value in manifest.items()):
            raise RuntimeError("The lab handoff differs from the stored release receipt.")
        return version, receipt, [landing], "reused"
    write_json(receipt, {**manifest, "received_utc": timestamp()})
    return version, receipt, [landing], "created"


def validate_config(config):
    columns = config["columns"]
    names = [c["name"] for c in columns]
    source_names = [c["source"] for c in columns]
    if not names or len(names) != len(set(names)) or len({s.casefold() for s in source_names}) != len(source_names):
        raise ValueError("Configure unique source and normalized column names.")
    kinds = {c["name"]: c["type"] for c in columns}
    if any(not IDENTIFIER.fullmatch(name) or name == "label" or kind not in KINDS for name, kind in kinds.items()):
        raise ValueError("Use lowercase identifiers and supported types. The final name 'label' is reserved.")
    if config["format"] not in {"csv", "json", "parquet"}:
        raise ValueError("Use csv, json (JSON Lines) or parquet.")
    if not IDENTIFIER.fullmatch(config["dataset_id"]):
        raise ValueError("Use a lowercase dataset identifier, such as project_data.")
    feature_version = config["feature_version"]
    if not re.fullmatch(r"features_v[1-9][0-9]*", feature_version):
        raise ValueError("Use a feature version such as features_v1.")
    if not config["source_url"].strip() or not config["prediction_time"].strip():
        raise ValueError("Document source_url and prediction_time.")
    keys, target = config["keys"], config["target"]
    if not keys or len(keys) != len(set(keys)) or target in keys or (set(keys) | {target}) - kinds.keys():
        raise ValueError("Provide unique configured key columns and a different configured target.")
    if config["task"] not in {"classification", "regression"}:
        raise ValueError("Set task to classification or regression.")
    if config["task"] == "regression" and kinds[target] not in NUMERIC:
        raise ValueError("Regression requires a numeric target.")
    if not 0 <= config["max_reject_fraction"] <= 1:
        raise ValueError("max_reject_fraction must be between 0 and 1.")
    for column in columns:
        has_bounds = "min" in column or "max" in column
        if has_bounds and column["type"] not in NUMERIC:
            raise ValueError(f"Use numeric bounds only on numeric columns: {column['name']}")
        if has_bounds and column.get("min", float("-inf")) > column.get("max", float("inf")):
            raise ValueError(f"Invalid bounds on {column['name']}")
    profile = config["profile_numeric"]
    if len(profile) > 8 or len(profile) != len(set(profile)) or any(kinds.get(name) not in NUMERIC for name in profile):
        raise ValueError("Choose up to eight distinct configured numeric columns to profile.")
    if config.get("stock_forecast"):
        expected = {
            "open_price": ("identity", ["open"]), "previous_close": ("lag", ["close"]),
            "mean_close_previous_5": ("trailing_mean", ["close"]),
            "previous_range_fraction": ("previous_range_fraction", ["high", "low", "close"]),
            "log_previous_volume": ("log_lag", ["volume"]),
        }
        configured = {f["name"]: (f["operation"], f["sources"]) for f in config["features"]}
        if (keys != ["ticker", "date"] or target != "close" or config["format"] != "parquet"
                or configured != expected or len(config["features"]) != len(expected)
                or any(not f[field].strip() for f in config["features"]
                       for field in ("definition", "available_at", "justification"))):
            raise ValueError("The stock forecast requires its documented keys, Parquet format and feature definitions.")
        return
    feature_names = set(keys) | {"label", target}
    if not config["features"]:
        raise ValueError("Configure at least one feature.")
    for feature in config["features"]:
        name, op, sources = feature["name"], feature["operation"], feature["sources"]
        if not IDENTIFIER.fullmatch(name) or name in feature_names:
            raise ValueError(f"Invalid or repeated feature name: {name}")
        if op not in FEATURES or len(sources) != FEATURES[op] or any(s not in kinds or s == target for s in sources):
            raise ValueError(f"Invalid feature inputs or operation: {name}")
        if op in {"log1p", "ratio"} and any(kinds[s] not in NUMERIC for s in sources):
            raise ValueError(f"Numeric feature requires numeric sources: {name}")
        if op in {"year", "month"} and kinds[sources[0]] not in {"date", "timestamp"}:
            raise ValueError(f"Date feature requires a date or timestamp: {name}")
        if not all(feature[field].strip() for field in ("definition", "available_at", "justification")):
            raise ValueError(f"Document definition, availability and justification for {name}.")
        feature_names.add(name)


def spark_path(path):
    # Spark needs a volume URI on Azure and a POSIX path for the local fixture.
    return "dbfs:" + str(path) if str(path).startswith("/Volumes/") else str(path)


def read_source(spark, raw_paths, config):
    reader = spark.read.option("mode", "FAILFAST")
    if config["format"] == "csv":
        options = config.get("csv_options", {})
        allowed = {"sep", "encoding", "multiLine", "quote", "escape"}
        if options.keys() - allowed:
            raise ValueError("CSV options allowed: sep, encoding, multiLine, quote and escape.")
        reader = reader.options(header=True, enforceSchema=False, **options)
    # PySpark takes a list for CSV/JSON but a variable argument list for Parquet.
    paths = list(map(spark_path, raw_paths))
    if config["format"] == "parquet":
        return reader.option("mergeSchema", "true").parquet(*paths)
    return getattr(reader, config["format"])(paths)


def clean(source, config):
    keys = config["keys"]
    cols = config["columns"]
    required = set(keys) | {config["target"]}
    source_names = [column["source"] for column in cols]
    missing = set(source_names) - set(source.columns)
    if missing or len({name.casefold() for name in source.columns}) != len(source.columns):
        raise ValueError(f"Schema mismatch. Absent configured headers: {sorted(missing)}. Check duplicate source headers.")
    if config.get("stock_forecast"):
        return clean_stock(source, cols, keys, required)
    selected = []
    for item in cols:
        escaped = item["source"].replace("`", "``")
        selected.append(F.col(f"`{escaped}`").cast("string").alias(item["name"]))
    raw = source.select(*selected)
    raw = raw.withColumn("_raw_record", F.to_json(F.struct(*raw.columns), {"ignoreNullFields": "false"}))
    expressions, reasons, audit = [], [], []
    null_tokens = [token.strip().lower() for token in config.get("null_tokens", [])]
    null_tokens = sorted(set(null_tokens) | {""})
    for item in cols:
        name, kind = item["name"], item["type"]
        text = F.trim(F.col(name))
        absent = text.isNull() | F.lower(text).isin(null_tokens)
        converted = text if kind == "string" else F.expr(f"try_cast(trim(`{name}`) AS {kind})")
        invalid = converted.isNull()
        if kind == "double":
            invalid = invalid | F.isnan(converted) | (F.abs(converted) == float("inf"))
        if "min" in item:
            invalid = invalid | (converted < item["min"])
        if "max" in item:
            invalid = invalid | (converted > item["max"])
        if "allowed" in item:
            invalid = invalid | (~converted.isin(item["allowed"]))
        is_invalid = ~absent & invalid
        audit.extend([absent.alias(f"_missing_{name}"), is_invalid.alias(f"_invalid_{name}")])
        expressions.append(F.when(~absent & ~invalid, converted).otherwise(F.lit(None).cast(kind)).alias(name))
        reasons.append(F.when(is_invalid, F.lit(f"invalid_{name}")))
        if item.get("required") or name in required:
            reasons.append(F.when(absent, F.lit(f"missing_{name}")))
    typed = raw.select("_raw_record", *expressions, *audit, F.concat_ws("|", *reasons).alias("_reason"))
    fields = [item["name"] for item in cols]
    signature = F.sha2(F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}), 256)
    by_key = Window.partitionBy(*keys)
    ordered = by_key.orderBy(F.col("_signature"), F.col("_raw_record"))
    valid = typed.where(F.col("_reason") == "").withColumn("_signature", signature)
    duplicates = valid.withColumn("_row_number", F.row_number().over(ordered))
    duplicates = duplicates.withColumn(
        "_reason",
        F.when(F.min("_signature").over(by_key) != F.max("_signature").over(by_key), "conflicting_key")
        .when(F.col("_row_number") > 1, "exact_duplicate").otherwise(""),
    ).drop("_signature", "_row_number")
    return typed.where(F.col("_reason") != "").unionByName(duplicates)


def clean_stock(source, cols, keys, required):
    for column in cols:
        if source.schema[column["source"]].dataType not in (KINDS[column["type"]], T.StringType()):
            raise ValueError(f"Unexpected Parquet type for {column['source']}.")
    df = source.select(*[F.col(column["source"]).alias(column["name"]) for column in cols])
    typed_columns, parsed, audit, reasons = [], {}, [], []
    for column in cols:
        name = column["name"]
        kind = column["type"]
        value = F.col(name)
        absent = value.isNull()
        if df.schema[name].dataType == T.StringType():
            absent = absent | (F.trim(value) == "")
            value = F.trim(value) if kind == "string" else F.expr(f"try_cast(trim(`{name}`) AS {kind})")
        invalid = value.isNull()
        if kind == "double":
            invalid = invalid | F.isnan(value) | (F.abs(value) == float("inf"))
            if "min" in column:
                invalid = invalid | (value < column["min"])
        invalid = ~absent & invalid
        value = F.when(~absent, value)
        parsed[name] = value
        typed_columns.append(value.alias(name))
        audit.extend([absent.alias(f"_missing_{name}"),
                      F.coalesce(invalid, F.lit(False)).alias(f"_invalid_{name}")])
        reasons.append(F.when(invalid, F.lit(f"invalid_{name}")))
        if column.get("required") or name in required:
            reasons.append(F.when(absent, F.lit(f"missing_{name}")))

    prices = [parsed[name] for name in ("open", "high", "low", "close")]
    open_price, high, low, close = prices
    tolerance = high * 1e-6
    impossible_range = (high < low) | (F.greatest(open_price, close) > high + tolerance) | (
        F.least(open_price, close) < low - tolerance)
    reasons.extend([F.when(F.least(*prices) <= 0, F.lit("nonpositive_price")),
                    F.when(impossible_range, F.lit("inconsistent_ohlc"))])
    reason = F.concat_ws("|", *reasons)
    raw_record = F.when(reason != "", F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}))
    typed = df.select(*typed_columns, *audit, reason.alias("_reason"), raw_record.alias("_raw_record"))

    # Compare native values directly and serialize only quarantined records.
    by_key = Window.partitionBy(*keys, F.col("_reason") == "")
    values = F.struct(*[column["name"] for column in cols if column["name"] not in keys])
    status = F.when(F.col("_reason") != "", F.col("_reason")).when(
        F.min(values).over(by_key) != F.max(values).over(by_key), "conflicting_key"
    ).when(F.row_number().over(by_key.orderBy(F.lit(1))) > 1, "exact_duplicate").otherwise("")
    return typed.withColumn("_reason", status)


def stock_features(notebook_data):
    by_ticker_date = Window.partitionBy("ticker").orderBy("date")
    prior_five = by_ticker_date.rowsBetween(-5, -1)
    previous_high = F.lag("high").over(by_ticker_date)
    previous_low = F.lag("low").over(by_ticker_date)
    previous_close = F.lag("close").over(by_ticker_date)
    return notebook_data.select(
        "ticker", "date", F.col("open").alias("open_price"),
        previous_close.alias("previous_close"),
        F.when(F.count("close").over(prior_five) == 5, F.avg("close").over(prior_five)).alias("mean_close_previous_5"),
        ((previous_high - previous_low) / previous_close).alias("previous_range_fraction"),
        F.log1p(F.lag("volume").over(by_ticker_date)).alias("log_previous_volume"),
        F.col("close").alias("label"),
    )


def feature_frame(df, config):
    if config.get("stock_forecast"):
        return stock_features(df)
    columns = [F.col(name) for name in config["keys"]]
    for spec in config["features"]:
        op = spec["operation"]
        first = F.col(spec["sources"][0])
        if op == "identity":
            value = first
        elif op == "log1p":
            value = F.when(first >= 0, F.log1p(first))
        elif op == "year":
            value = F.year(first)
        elif op == "month":
            value = F.month(first)
        elif op == "missing_flag":
            value = first.isNull().cast("int")
        else:
            second = F.col(spec["sources"][1])
            value = F.when(second != 0, first.cast("double") / second)
            value = F.when(F.abs(value) != float("inf"), value)
        columns.append(value.alias(spec["name"]))
    return df.select(*columns, F.col(config["target"]).alias("label"))


def profile(df, config, rows):
    target = config["target"]
    numeric = list(config["profile_numeric"])
    if config["task"] == "regression" and target not in numeric:
        numeric.append(target)
    percentiles = [0.0, 0.25, 0.5, 0.75, 1.0]
    if numeric:
        summaries = df.agg(*[F.percentile_approx(F.col(name), percentiles, 1000).alias(name)
                             for name in numeric]).first().asDict()
        quantiles = [summaries[name] or [] for name in numeric]
    else:
        quantiles = []
    aggregations, distributions, pairs = [], {}, {}
    for index, (name, values) in enumerate(zip(numeric, quantiles)):
        aggregations.append(F.count(name).alias(f"n_{index}"))
        if values:
            q1, q3 = values[1], values[3]
            low, high = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
            aggregations.append(F.count(F.when((F.col(name) < low) | (F.col(name) > high), 1)).alias(f"o_{index}"))
        distributions[name] = {"approx_quantiles": dict(zip(map(str, percentiles), values))}
        for other in range(index):
            ident = f"c_{other}_{index}"
            pairs[ident] = f"{numeric[other]} vs {name}"
            aggregations.append(F.corr(numeric[other], name).alias(ident))
    if aggregations:
        measured = df.agg(*aggregations).first().asDict()
        for index, name in enumerate(numeric):
            distributions[name].update(non_null=measured[f"n_{index}"],
                                       iqr_outliers=measured.get(f"o_{index}"))
    else:
        measured = {}
    counts = []
    if config["task"] == "classification":
        counts = [row.asDict() for row in df.groupBy(target).count()
                  .orderBy(F.desc("count"), F.asc(target)).limit(20).collect()]
    correlations = {}
    for key, pair in pairs.items():
        value = measured[key]
        correlations[pair] = value if value is not None and -1.00000001 <= value <= 1.00000001 else None
    return {"accepted_population": rows, "sampled": False, "target": target, "top_20_target_counts": counts,
            "other_target_rows": rows - sum(row["count"] for row in counts) if counts else None,
            "numeric": distributions, "pearson": correlations,
            "notes": "Quantiles are approximate over all accepted rows. Pearson uses available pairs."}


def stock_coverage(df):
    coverage = df.agg(
        F.min("date").alias("first_date"), F.max("date").alias("last_date"),
        F.countDistinct("ticker").alias("ticker_count"), F.countDistinct("date").alias("distinct_dates"),
        F.count(F.when(F.col("volume") == 0, 1)).alias("zero_volume_rows"),
        F.count(F.when(F.col("stock_splits") > 0, 1)).alias("split_action_rows"),
        F.count(F.when(F.col("dividends") > 0, 1)).alias("dividend_action_rows"),
    ).first().asDict()
    coverage["rows_by_year"] = [row.asDict() for row in df.groupBy(F.year("date").alias("year"))
                                .count().orderBy("year").collect()]
    return coverage


def prepare(spark, run, root, config, landing, preserved, clean_path, clean_format, report, handoff_path):
    config_hash = hashlib.sha256(json_bytes(config)).hexdigest()
    release = config.get("csv_release") if report["scenario"] == "full" else None
    if isinstance(landing, LabPath):
        version, receipt, data_paths, ingest_status = ingest_lab_release(landing, root, config, handoff_path, report)
    elif release:
        version, receipt, data_paths, ingest_status = ingest_csv_release(landing, root, config, preserved)
    else:
        version, receipt, data_paths, ingest_status = ingest(landing, root, config, preserved)
    output_version = hashlib.sha256(json_bytes(
        [version, config_hash, report["code_version"], clean_format, str(clean_path.parent.parent)]
    )).hexdigest()
    marker = root / "catalog" / f"{output_version}.json"
    report.update(input_version=version, manifest=str(receipt), ingestion=ingest_status,
                  config_sha256=config_hash, candidate_output_version=output_version, clean_format=clean_format)
    if marker.exists():
        previous = read_json(marker)
        if not all(project_path(previous["outputs"][name]).is_dir() for name in ("clean", "features")):
            raise RuntimeError("Previously published clean data or features are missing.")
        report.update(status="reused", output_version=output_version, catalog=str(marker),
                      original_run_id=previous["published_run"], row_counts=previous["row_counts"])
        return

    source = read_source(spark, data_paths, config)
    data = clean(source, config).persist(StorageLevel.DISK_ONLY)
    try:
        audit_names = [name for name in data.columns if name.startswith(("_missing_", "_invalid_"))]
        stats = data.agg(F.count("*").alias("rows_in"),
                         F.count(F.when(F.col("_reason") == "", 1)).alias("accepted"),
                         *[F.count(F.when(F.col(name), 1)).alias(name) for name in audit_names]).first().asDict()
        total, accepted = stats.pop("rows_in"), stats.pop("accepted")
        rejected = data.where(F.col("_reason") != "")
        row_counts = {"input": total, "accepted": accepted, "rejected": total - accepted}
        failures = []
        reject_fraction = (total - accepted) / total if total else None
        if release and total != release["rows"]:
            failures.append(f"Converted Parquet row count {total} differs from the source ({release['rows']}).")
        if not accepted:
            failures.append("No accepted records")
        if reject_fraction is not None and reject_fraction > config["max_reject_fraction"]:
            failures.append("Reject fraction exceeds threshold")
        if report["scenario"] == "quality_test" and not failures:
            failures.append("Invalid test design: the test input passed all data rules")
        quality_path = run / "quality.json"
        write_json(quality_path, {
            "row_counts": row_counts, "reject_fraction": reject_fraction, "failures": failures,
            "expected_source_rows": release["rows"] if release else None,
            "max_reject_fraction": config["max_reject_fraction"], "column_missing_and_invalid": stats,
            "reject_reason_counts": [r.asDict() for r in rejected.groupBy("_reason").count().collect()],
        })
        if config.get("stock_forecast"):
            rejected = rejected.withColumn("_raw_record", F.coalesce(F.col("_raw_record"), F.to_json(
                F.struct(*[item["name"] for item in config["columns"]]), {"ignoreNullFields": "false"})))
        rejected.select(*config["keys"], "_raw_record", "_reason").write.mode("errorifexists").parquet(
            spark_path(run / "rejected"))
        if not failures:
            data.where(F.col("_reason") == "").select(*[item["name"] for item in config["columns"]]) \
                .write.mode("errorifexists").format(clean_format).save(spark_path(clean_path))
        report.update(status="prepared", row_counts=row_counts, quality=str(quality_path),
                      failures=failures, candidate_clean_path=str(clean_path))
    finally:
        data.unpersist(blocking=False)


def validate(report):
    if report["status"] == "reused":
        return
    failures = report["failures"]
    report["status"] = "quality_failed" if failures else "validated"
    if failures:
        raise RuntimeError("; ".join(failures))


def publish(spark, run, root, config, report):
    if report["status"] == "reused":
        return
    if report["status"] != "validated":
        raise RuntimeError("Publication requires a successful validation task.")
    clean_path = project_path(report["candidate_clean_path"])
    feature_path = run / config["feature_version"]
    accepted = spark.read.format(report["clean_format"]).load(spark_path(clean_path))
    approved_count = report["row_counts"]["accepted"]
    if accepted.count() != approved_count:
        raise RuntimeError("Saved clean row count does not match the quality report.")
    eda = profile(accepted, config, approved_count)
    if config.get("stock_forecast"):
        eda["coverage"] = stock_coverage(accepted)
    features = feature_frame(accepted, config)
    features.write.mode("errorifexists").parquet(spark_path(feature_path))
    saved = spark.read.parquet(spark_path(feature_path))
    counts = saved.agg(F.count("*").alias("feature_rows"), *[
        F.count(F.when(F.col(item["name"]).isNull(), 1)).alias(item["name"])
        for item in config["features"]
    ]).first().asDict()
    if counts.pop("feature_rows") != approved_count:
        raise RuntimeError("Saved feature row count does not match the quality report.")
    eda["feature_missing_counts"] = counts
    eda_path = run / "eda.json"
    write_json(eda_path, eda)
    output_version = report["candidate_output_version"]
    marker = root / "catalog" / f"{output_version}.json"
    write_json(marker, {
        "dataset_id": config["dataset_id"], "source_url": config["source_url"],
        "scenario": report["scenario"], "published_run": report["run_id"], "published_utc": timestamp(),
        "input_version": report["input_version"], "output_version": output_version,
        "code_version": report["code_version"], "adf_run_id": report["adf_run_id"],
        "config_sha256": report["config_sha256"], "feature_version": config["feature_version"],
        "manifest": report["manifest"], "quality": report["quality"], "eda": str(eda_path),
        "clean_format": report["clean_format"], "keys": config["keys"], "target": config["target"],
        "model_label": "label", "prediction_time": config["prediction_time"], "row_counts": report["row_counts"],
        "assumptions": config.get("assumptions", []), "columns": config["columns"],
        "clean_schema": accepted.schema.jsonValue(), "feature_schema": saved.schema.jsonValue(),
        "features": config["features"], "outputs": {
            "clean": str(clean_path), "features": str(feature_path), "rejected": str(run / "rejected"),
        },
    })
    report.update(status="published", output_version=output_version, catalog=str(marker))


def execute(spark, config_path, landing_path, root_path, scenario, run_id, code_version,
            stage="all", preserved_source_path="", processed_root="", clean_format="parquet", adf_run_id="",
            handoff_path=""):
    config = read_json(project_path(config_path))
    validate_config(config)
    if scenario not in {"full", "quality_test", "demo"} or not RUN_ID.fullmatch(run_id):
        raise ValueError("Invalid scenario or run_id.")
    if not code_version.strip() or clean_format not in {"parquet", "delta"}:
        raise ValueError("Set a code version and either parquet or delta for the clean output.")
    if stage not in {"all", "prepare", "validate", "publish"}:
        raise ValueError("Unknown job task stage.")
    name = config["dataset_id"]
    root = project_path(root_path) / name / scenario
    run = root / "runs" / run_id
    clean_path = (project_path(processed_root) / name / scenario / run_id if processed_root else run) / "clean"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    stages = ("prepare", "validate", "publish") if stage == "all" else (stage,)
    for step in stages:
        started = perf_counter()
        if step == "prepare":
            run.mkdir(parents=True, exist_ok=False)
            report = {"run_id": run_id, "scenario": scenario, "code_version": code_version,
                      "adf_run_id": adf_run_id or None, "started_utc": timestamp(), "stage_seconds": {}}
        else:
            source_report = "prepare.json" if step == "validate" else "gate.json"
            report = read_json(run / source_report)
            if report["config_sha256"] != hashlib.sha256(json_bytes(config)).hexdigest():
                raise RuntimeError("Configuration changed after preparation.")
        try:
            if step == "prepare":
                preserved = project_path(preserved_source_path) if preserved_source_path else None
                prepare(spark, run, root, config, project_path(landing_path), preserved,
                        clean_path, clean_format, report, handoff_path)
            elif step == "validate":
                validate(report)
            else:
                publish(spark, run, root, config, report)
        except Exception as error:
            if report.get("status") != "quality_failed":
                report["status"] = "error"
            report["error"] = str(error)[:3000]
            raise
        finally:
            report["stage_seconds"][step] = round(perf_counter() - started, 3)
            report["elapsed_seconds"] = round(sum(report["stage_seconds"].values()), 3)
            if step != "publish":
                write_json(run / ("prepare.json" if step == "prepare" else "gate.json"), report)
            if step == "publish" or report.get("status") in {"quality_failed", "error"}:
                write_json(run / "run.json", report)
            print(json.dumps(report, indent=2))
    return report


def notebook_run():
    defaults = {"config_path": "", "landing_path": "", "root_path": "",
                "scenario": "full", "run_id": "", "code_version": "", "stage": "all",
                "preserved_source_path": "", "processed_root": "", "clean_format": "delta", "adf_run_id": "",
                "handoff_path": ""}
    for name, value in defaults.items():
        dbutils.widgets.text(name, value)
    settings = {name: dbutils.widgets.get(name).strip() for name in defaults}
    required = ("config_path", "landing_path", "root_path", "code_version")
    allowed = ("abfss://", "/Volumes/")
    if any(not settings[name].startswith(allowed) for name in required[:3]) or not settings["code_version"]:
        raise ValueError("Supply the three Azure paths and a code_version.")
    for name in ("processed_root", "preserved_source_path", "handoff_path"):
        if settings[name] and not settings[name].startswith(allowed):
            raise ValueError(f"{name} must be a governed Azure path.")
    if not settings["run_id"]:
        settings["run_id"] = "manual-" + uuid4().hex
    execute(spark, **settings)


def local_run():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config-path", "landing-path", "root-path", "scenario", "run-id", "code-version"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--stage", choices=("all", "prepare", "validate", "publish"), default="all")
    parser.add_argument("--processed-root", default="")
    parser.add_argument("--preserved-source-path", default="")
    parser.add_argument("--adf-run-id", default="")
    parser.add_argument("--handoff-path", default="")
    parser.add_argument("--local-threads", type=int, default=2)
    parser.add_argument("--local-shuffle-partitions", type=int, default=4)
    options = vars(parser.parse_args())
    threads = options.pop("local_threads")
    partitions = options.pop("local_shuffle_partitions")
    spark_session = (SparkSession.builder.master(f"local[{threads}]").appName("DSAI3202 Phase 1")
                     .config("spark.sql.shuffle.partitions", partitions).getOrCreate())
    spark_session.sparkContext.setLogLevel("ERROR")
    try:
        execute(spark_session, **options)
    finally:
        spark_session.stop()


if "dbutils" in globals() and "spark" in globals():
    notebook_run()
elif __name__ == "__main__":
    local_run()
