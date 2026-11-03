import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    wh = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]").appName("utility-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.warehouse.dir", str(wh))
         .config("spark.sql.ansi.enabled", "true")        # same strictness as Databricks serverless
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
