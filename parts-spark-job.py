import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


USER_PATH = "denis-scherbina-xlc3353"
SUPPLIERS_PATH = 's3a://de-raw/supplier'
NATION_PATH = 's3a://de-raw/nation'
PART_PATH = 's3a://de-raw/part'
PARTSUPP_PATH = 's3a://de-raw/partsupp'
TARGET_PATH = f"s3a://de-project/{USER_PATH}/parts_report"
ACCESS_KEY = ''
SECRET_KEY = ''
ENDPOINT = ''
REGION = ''


def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', ENDPOINT)
            .config('spark.hadoop.fs.s3a.region', REGION)
            .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY)
            .config('spark.hadoop.fs.s3a.secret.key', SECRET_KEY)
            .getOrCreate())


def main():
    spark = _spark_session()
    part_df = spark.read.parquet(PART_PATH)
    partsupp_df = spark.read.parquet(PARTSUPP_PATH)
    supplier_df = spark.read.parquet(SUPPLIERS_PATH)
    nation_df = spark.read.parquet(NATION_PATH)

    part_df = part_df \
        .join(other=partsupp_df, on=F.col('P_PARTKEY') == F.col('PS_PARTKEY'), how='inner') \
        .join(other=supplier_df, on=F.col('PS_SUPPKEY') == F.col('S_SUPPKEY'), how='inner') \
        .join(other=nation_df, on=F.col('S_NATIONKEY') == F.col('N_NATIONKEY'), how='inner') \
        .groupBy(F.col('N_NAME'), F.col('P_TYPE'), F.col('P_CONTAINER')) \
        .agg(F.count(F.col('P_PARTKEY')).alias('parts_count'),
             F.avg(F.col('P_RETAILPRICE')).alias('avg_retailprice'),
             F.sum(F.col('P_SIZE')).alias('size'),
             F.mean(F.col('P_RETAILPRICE')).alias('mean_retailprice'),
             F.min(F.col('P_RETAILPRICE')).alias('min_retailprice'),
             F.max(F.col('P_RETAILPRICE')).alias('max_retailprice'),
             F.avg(F.col('PS_SUPPLYCOST')).alias('avg_supplycost'),
             F.mean(F.col('PS_SUPPLYCOST')).alias('mean_supplycost'),
             F.min(F.col('PS_SUPPLYCOST')).alias('min_supplycost'),
             F.max(F.col('PS_SUPPLYCOST')).alias('max_supplycost')) \
        .select(F.col('N_NAME'), F.col('P_TYPE'), F.col('P_CONTAINER'), F.col('parts_count'),
                F.col('avg_retailprice'), F.col('size'), F.col('mean_retailprice'), F.col('min_retailprice'),
                F.col('max_retailprice'), F.col('avg_supplycost'), F.col('mean_supplycost'),
                F.col('min_supplycost'), F.col('max_supplycost')) \
        .orderBy(F.col('N_NAME'), F.col('P_TYPE'), F.col('P_CONTAINER'))

    part_df.write.mode('overwrite').parquet(TARGET_PATH)
    # part_df.coalesce(4).write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
