import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


USER_PATH = "denis-scherbina-xlc3353"
DATA_PATH = f"s3a://de-raw/lineitem"
TARGET_PATH = f"s3a://de-project/{USER_PATH}/lineitems_report"
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
    lineitems_df = spark.read.parquet(DATA_PATH)
    lineitems_df = lineitems_df \
        .select(F.col('L_ORDERKEY'), F.col('L_PARTKEY'), F.col('L_EXTENDEDPRICE'), F.col('L_DISCOUNT'),
                F.col('L_TAX'),
                F.datediff(F.to_date(F.col('L_RECEIPTDATE')), F.to_date(F.col('L_SHIPDATE'))).alias('dt_diff'),
                F.when(F.col('L_RETURNFLAG') == 'A', 1).otherwise(0).alias('A_return_flags'),
                F.when(F.col('L_RETURNFLAG') == 'R', 1).otherwise(0).alias('R_return_flags'),
                F.when(F.col('L_RETURNFLAG') == 'N', 1).otherwise(0).alias('N_return_flags'),
                ) \
        .groupBy(F.col('L_ORDERKEY')) \
        .agg(F.count(F.col('L_PARTKEY')).alias('count'),
             F.sum(F.col('L_EXTENDEDPRICE')).alias('sum_extendprice'),
             F.mean(F.col('L_DISCOUNT')).alias('mean_discount'),
             F.mean(F.col('L_TAX')).alias('mean_tax'),
             F.mean(F.col('dt_diff')).alias('delivery_days'),
             F.sum(F.col('A_return_flags')).alias('A_return_flags'),
             F.sum(F.col('R_return_flags')).alias('R_return_flags'),
             F.sum(F.col('N_return_flags')).alias('N_return_flags')
             ) \
        .select(F.col('L_ORDERKEY'), F.col('count'), F.col('sum_extendprice'), F.col('mean_discount'),
                F.col('mean_tax'), F.col('delivery_days'), F.col('A_return_flags'),
                F.col('R_return_flags'), F.col('N_return_flags')) \
        .orderBy(F.col('L_ORDERKEY'))

    lineitems_df.write.mode('overwrite').parquet(TARGET_PATH)
    # lineitems_df.coalesce(4).write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
