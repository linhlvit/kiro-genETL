"""hash_id UDF — SHA-256 based surrogate key generator.

Usage in Spark SQL: hash_id('SOURCE_SYSTEM', col1)
Implemented as sha2(concat_ws('|', args...), 256) for native Spark SQL compatibility.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def register_hash_id(spark: SparkSession) -> None:
    """Register hash_id as a native Spark SQL function.

    Uses sha2(concat_ws('|', ...)) which works on both classic and Connect modes.
    Registers overloads for 2 and 3 arguments.
    """
    spark.sql(
        "CREATE OR REPLACE TEMPORARY FUNCTION hash_id(a STRING, b STRING) "
        "RETURNS STRING "
        "RETURN sha2(concat_ws('|', a, CAST(b AS STRING)), 256)"
    )
