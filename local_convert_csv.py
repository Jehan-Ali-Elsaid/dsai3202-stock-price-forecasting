"""Reproduce the ADF CSV-to-string-Parquet mapping for local verification."""
import argparse
import json
from os import link
from pathlib import Path
from tempfile import NamedTemporaryFile

import pyarrow as pa
from pyarrow import csv, parquet


def convert(source, destination, config):
    if destination.exists():
        raise FileExistsError(destination)
    columns = config["columns"]
    raw_names = [column.get("raw_source", column["source"]) for column in columns]
    bronze_names = [column["source"] for column in columns]
    parsed = csv.open_csv(
        source,
        read_options=csv.ReadOptions(block_size=32 * 1024 * 1024),
        parse_options=csv.ParseOptions(double_quote=True),
        convert_options=csv.ConvertOptions(
            column_types={name: pa.string() for name in raw_names},
            strings_can_be_null=True, null_values=[""],
        ),
    )
    if parsed.schema.names != raw_names:
        raise ValueError("CSV headers differ from the ADF source mapping.")
    schema = pa.schema([(name, pa.string()) for name in bronze_names])
    destination.parent.mkdir(exist_ok=True, parents=True)
    rows = 0
    with NamedTemporaryFile(dir=destination.parent, suffix=".parquet.part", delete=False) as stream:
        pending = Path(stream.name)
        try:
            with parquet.ParquetWriter(stream, schema, compression="snappy") as writer:
                for batch in parsed:
                    writer.write_batch(pa.RecordBatch.from_arrays(batch.columns, schema=schema))
                    rows += batch.num_rows
            stream.close()
            link(pending, destination)
        finally:
            stream.close()
            pending.unlink()
    return rows


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("source", type=Path)
    cli.add_argument("destination", type=Path)
    cli.add_argument("--config", type=Path, default=Path(__file__).with_name("project.json"))
    args = cli.parse_args()
    print(f"Converted {convert(args.source, args.destination, json.loads(args.config.read_text())):,} rows.")
