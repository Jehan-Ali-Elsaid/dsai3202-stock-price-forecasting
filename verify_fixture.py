"""Exercise publication, idempotency and prediction-time features on small Parquet fixtures."""
from datetime import date
import math
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession, functions as F

from make_test_data import make_fixtures
from phase1_pipeline import clean, execute, read_json


def verify(spark):
    config = Path(__file__).with_name("project.json")
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        fixtures = root / "fixtures"
        make_fixtures(fixtures)
        good, bad = fixtures / "valid.parquet", fixtures / "quality_test.parquet"
        first = execute(spark, config, good, root, "demo", "fixture-pass", "stock-phase1-v1")
        repeated = execute(spark, config, good, root, "demo", "fixture-rerun", "stock-phase1-v1")
        assert first["status"] == "published"
        assert first["row_counts"] == {"input": 14, "accepted": 14, "rejected": 0}
        assert repeated["status"] == "reused" and repeated["ingestion"] == "reused"
        assert all(first[k] == repeated[k] for k in ("input_version", "output_version", "manifest", "catalog"))
        assert len(list((root / "stock_daily" / "demo" / "raw").iterdir())) == 1
        entry = read_json(Path(first["catalog"]))
        rows = {(r.ticker, r.date): r for r in spark.read.parquet(entry["outputs"]["features"]).collect()}
        target = rows["AAA", date(2024, 1, 7)]
        assert target.open_price == 105 and target.label == 106
        assert target.previous_close == 105 and target.mean_close_previous_5 == 103
        assert math.isclose(target.previous_range_fraction, 5 / 105)
        assert target.log_previous_volume is None
        assert math.isclose(rows["AAA", date(2024, 1, 8)].log_previous_volume, math.log1p(105))
        assert rows["BBB", date(2024, 1, 7)].mean_close_previous_5 == 203
        assert rows["AAA", date(2024, 1, 6)].mean_close_previous_5 is None
        assert rows["BBB", date(2024, 1, 2)].previous_close is None
        original = spark.read.parquet(str(good))
        duplicated = original.where("Ticker = 'AAA' AND Date = DATE '2024-01-02'")
        conflict = original.where("Ticker = 'BBB' AND Date = DATE '2024-01-02'").withColumn(
            "Volume", F.lit(99.0))
        mixed = original.unionByName(duplicated).unionByName(conflict)
        reasons = {row["_reason"]: row["count"] for row in clean(mixed, read_json(config))
                   .groupBy("_reason").count().collect()}
        assert reasons == {"": 13, "exact_duplicate": 1, "conflicting_key": 2}
        # ADF stores bronze CSV fields as strings so Spark can quarantine failed casts.
        bronze = original.select(*[F.col(name).cast("string").alias(name) for name in original.columns])
        assert clean(bronze, read_json(config)).where("_reason = ''").count() == 14
        invalid_text = bronze.limit(1).withColumn("Open", F.lit("unparseable"))
        rejected_text = clean(invalid_text, read_json(config)).first()
        assert rejected_text["_invalid_open"] and "invalid_open" in rejected_text["_reason"]
        assert "unparseable" in rejected_text["_raw_record"]
        try:
            execute(spark, config, bad, root, "quality_test", "fixture-fail", "stock-phase1-v1")
        except RuntimeError as error:
            assert "No accepted records" in str(error)
        else:
            raise AssertionError("Bad data passed publication unexpectedly.")
        rejected = root / "stock_daily" / "quality_test"
        report = read_json(rejected / "runs" / "fixture-fail" / "run.json")
        assert report["status"] == "quality_failed"
        assert report["row_counts"] == {"input": 4, "accepted": 0, "rejected": 4}
        assert not (rejected / "catalog").exists()
        assert Path(first["catalog"]).exists()
    print("Stock fixture verified: per-ticker lags, warm-up, labels, rerun and quality failure.")


if __name__ == "__main__":
    session = (SparkSession.builder.master("local[2]").appName("Phase 1 fixture checks")
               .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    try:
        verify(session)
    finally:
        session.stop()
