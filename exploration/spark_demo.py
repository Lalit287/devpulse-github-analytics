"""Local PySpark verification on the same nested JSON sample used by Pandas."""
import argparse
import contextlib
import io
import os
import subprocess
import sys
from pathlib import Path

from config.settings import PROCESSED_DIR, SAMPLE_PATH
from exploration.io import write_json


def configure_java():
    """Prefer an existing Java 17/21 installation without global configuration."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    candidates = [
        Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
        Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    ]
    if sys.platform == "darwin":
        result = subprocess.run(["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True)
        if result.returncode == 0:
            candidates.append(Path(result.stdout.strip()))
    for candidate in candidates:
        if (candidate / "bin/java").is_file():
            os.environ["JAVA_HOME"] = str(candidate)
            return str(candidate)
    return None  # PySpark will try the java executable in PATH.


def create_spark():
    configure_java()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("DevPulse-Week1").master("local[2]")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_spark_demo(input_path=SAMPLE_PATH, output_dir=PROCESSED_DIR):
    from pyspark.sql import functions as F
    spark = create_spark()
    try:
        # Exercises both Python -> JVM conversion and an actual Python worker.
        smoke = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        smoke_sum = smoke.agg(F.sum("value").alias("sum")).first()["sum"]
        if smoke_sum != 6:
            raise RuntimeError("Spark smoke test returned the wrong sum")
        events = spark.read.option("mode", "FAILFAST").json(Path(input_path).resolve().as_uri()).cache()
        total = events.count()
        if not total:
            raise ValueError("Spark demonstration requires a nonempty JSON sample")
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            events.printSchema()
        selected = events.select(
            F.col("id").cast("string").alias("event_id"), F.col("type").alias("event_type"),
            F.col("repo.id").cast("string").alias("repository_id"), F.col("repo.name").alias("repository"),
            F.col("actor.id").cast("string").alias("actor_id"), F.col("actor.login").alias("account"),
            F.to_timestamp("created_at").alias("event_time_utc"), F.col("public"))
        selected.show(5, truncate=70)
        groups = selected.groupBy("event_type").count().orderBy("event_type")
        groups.show(truncate=False)
        result = {
            "status": "passed", "spark_version": spark.version, "master": spark.sparkContext.master,
            "java_home": os.environ.get("JAVA_HOME"), "python_worker": sys.executable,
            "smoke_sum": int(smoke_sum), "records": total,
            "unique_repositories": selected.select("repository_id").where("repository_id IS NOT NULL").distinct().count(),
            "unique_accounts": selected.select("actor_id").where("actor_id IS NOT NULL").distinct().count(),
            "event_type_counts": {row["event_type"]: row["count"] for row in groups.collect()},
            "transformed_examples": [row.asDict() for row in selected.select(
                "event_id", "event_type", "repository", "account",
                F.col("event_time_utc").cast("string").alias("event_time_utc")).limit(5).collect()],
        }
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "spark_schema.txt").write_text(captured.getvalue())
        write_json(output_dir / "spark_verification.json", result)
        events.unpersist()
        print(f"Inferred schema saved to {output_dir / 'spark_schema.txt'}")
        return result
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=SAMPLE_PATH)
    parser.add_argument("--output-dir", type=Path, default=PROCESSED_DIR)
    args = parser.parse_args()
    result = run_spark_demo(args.input, args.output_dir)
    print(f"Verified {result['records']:,} sampled records using {result['master']}.")


if __name__ == "__main__":
    main()
