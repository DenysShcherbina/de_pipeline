import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


USER_PATH = "denis-scherbina-xlc3353"
CUSTOMER_PATH = 's3a://de-raw/customer'
NATION_PATH = 's3a://de-raw/nation'
REGION_PATH = 's3a://de-raw/region'
TARGET_PATH = f"s3a://de-project/{USER_PATH}/customers_report"
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
    region_df = spark.read.parquet(REGION_PATH)
    customer_df = spark.read.parquet(CUSTOMER_PATH)
    nation_df = spark.read.parquet(NATION_PATH)
    customer_df = customer_df \
        .join(other=nation_df, on=F.col('C_NATIONKEY') == F.col('N_NATIONKEY'), how='left') \
        .join(other=region_df, on=F.col('N_REGIONKEY') == F.col('R_REGIONKEY'), how='left') \
        .groupBy(F.col('R_NAME'), F.col('N_NAME'), F.col('C_MKTSEGMENT')) \
        .agg(F.count(F.col('C_CUSTKEY')).alias('unique_customers_count'),
             F.avg(F.col('C_ACCTBAL')).alias('avg_acctbal'),
             F.mean(F.col('C_ACCTBAL')).alias('mean_acctbal'),
             F.min(F.col('C_ACCTBAL')).alias('min_acctbal'),
             F.max(F.col('C_ACCTBAL')).alias('max_acctbal')
             ) \
        .select(F.col('R_NAME'), F.col('N_NAME'), F.col('C_MKTSEGMENT'), F.col('unique_customers_count'),
                F.col('avg_acctbal'), F.col('mean_acctbal'), F.col('min_acctbal'), F.col('max_acctbal')) \
        .orderBy(F.col('N_NAME'), F.col('C_MKTSEGMENT'))

    customer_df.write.mode('overwrite').parquet(TARGET_PATH)
    # customer_df.coalesce(4).write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
