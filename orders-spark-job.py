import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


USER_PATH = "denis-scherbina-xlc3353"
ORDERS_PATH = 's3a://de-raw/orders'
CUSTOMER_PATH = 's3a://de-raw/customer'
NATION_PATH = 's3a://de-raw/nation'
TARGET_PATH = f"s3a://de-project/{USER_PATH}/orders_report"
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
    orders_df = spark.read.parquet(ORDERS_PATH)
    customer_df = spark.read.parquet(CUSTOMER_PATH)
    nation_df = spark.read.parquet(NATION_PATH)
    orders_df = orders_df \
        .join(other=customer_df, on=F.col('O_CUSTKEY') == F.col('C_CUSTKEY'), how='left') \
        .join(other=nation_df, on=F.col('C_NATIONKEY') == F.col('N_NATIONKEY'), how='left') \
        .withColumn('O_MONTH', F.date_format(F.col("O_ORDERDATE"), "yyyy-MM")) \
        .withColumn('f_order_status', F.when(F.col('O_ORDERSTATUS') == 'F', 1).otherwise(0)) \
        .withColumn('o_order_status', F.when(F.col('O_ORDERSTATUS') == 'O', 1).otherwise(0)) \
        .withColumn('p_order_status', F.when(F.col('O_ORDERSTATUS') == 'P', 1).otherwise(0)) \
        .groupBy(F.col('O_MONTH'), F.col('O_ORDERPRIORITY'), F.col('N_NAME')) \
        .agg(F.count(F.col('O_ORDERKEY')).alias('orders_count'),
             F.avg(F.col('O_TOTALPRICE')).alias('avg_order_price'),
             F.sum(F.col('O_TOTALPRICE')).alias('sum_order_price'),
             F.min(F.col('O_TOTALPRICE')).alias('min_order_price'),
             F.max(F.col('O_TOTALPRICE')).alias('max_order_price'),
             F.sum(F.col('f_order_status')).alias('f_order_status'),
             F.sum(F.col('o_order_status')).alias('o_order_status'),
             F.sum(F.col('p_order_status')).alias('p_order_status')) \
        .select(F.col('O_MONTH'), F.col('N_NAME'), F.col('O_ORDERPRIORITY'),
                F.col('orders_count'), F.col('avg_order_price'), F.col('sum_order_price'),
                F.col('min_order_price'), F.col('max_order_price'), F.col('f_order_status'),
                F.col('o_order_status'), F.col('p_order_status')) \
        .orderBy(F.col('N_NAME'), F.col('O_ORDERPRIORITY'))

    orders_df.write.mode('overwrite').parquet(TARGET_PATH)
    # orders_df.coalesce(4).write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
