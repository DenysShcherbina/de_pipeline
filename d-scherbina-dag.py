import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"
USER_PATH = "denis-scherbina-xlc3353"


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=f'submit_{task_id}',
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, link_dag):
    return SparkKubernetesSensor(
        task_id=f'sensor_{task_id}',
        namespace=K8S_SPARK_NAMESPACE,
        application_name=f"{{{{task_instance.xcom_pull(task_ids='submit_{task_id}')['metadata']['name']}}}}",
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        poke_interval=60,
        dag=link_dag
    )


with DAG(
        dag_id=f"de-project-{USER_PATH}-dag",
        schedule_interval=None,
        start_date=pendulum.datetime(2026, 4, 18, tz="UTC"),
        default_args={'owner': USER_PATH},
        tags=["de_project", "de-scherbina"],
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)

    lineitems_submit_task = _build_submit_operator(task_id='lineitems',
                                                   application_file='spark-submit-lineitems.yaml',
                                                   link_dag=dag)

    lineitems_sensor_task = _build_sensor(task_id='lineitems', link_dag=dag)
    lineitems_datamart_task = SQLExecuteQueryOperator(
        task_id='lineitems_datamart',
        conn_id=GREENPLUM_ID,
        sql=f'''
            DROP EXTERNAL TABLE IF EXISTS "{USER_PATH}".lineitems;
    
            CREATE EXTERNAL TABLE "{USER_PATH}".lineitems(
                L_ORDERKEY BIGINT, count BIGINT, sum_extendprice FLOAT8, mean_discount FLOAT8, mean_tax FLOAT8, 
                delivery_days FLOAT8, A_return_flags BIGINT, R_return_flags BIGINT, N_return_flags BIGINT)
	        LOCATION ('pxf://de-project/{USER_PATH}/lineitems_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
            ''',
        autocommit=True,
        split_statements=True,
        return_last=False)

    orders_submit_task = _build_submit_operator(task_id='orders',
                                                application_file='spark-submit-orders.yaml',
                                                link_dag=dag)

    orders_sensor_task = _build_sensor(task_id='orders', link_dag=dag)
    orders_datamart_task = SQLExecuteQueryOperator(
        task_id='orders_datamart',
        conn_id=GREENPLUM_ID,
        sql=f'''
                DROP EXTERNAL TABLE IF EXISTS "{USER_PATH}".orders;

                CREATE EXTERNAL TABLE "{USER_PATH}".orders(
                    O_MONTH TEXT, N_NAME TEXT, O_ORDERPRIORITY TEXT, orders_count BIGINT,
                    avg_order_price FLOAT8, sum_order_price FLOAT8, min_order_price FLOAT8,
                    max_order_price FLOAT8, f_order_status BIGINT, o_order_status BIGINT,
                    p_order_status BIGINT)
    	        LOCATION ('pxf://de-project/{USER_PATH}/orders_report?PROFILE=s3:parquet&SERVER=default')
                ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
                ''',
        autocommit=True,
        split_statements=True,
        return_last=False)

    customers_submit_task = _build_submit_operator(task_id='customers',
                                                   application_file='spark-submit-customers.yaml',
                                                   link_dag=dag)

    customers_sensor_task = _build_sensor(task_id='customers', link_dag=dag)
    customers_datamart_task = SQLExecuteQueryOperator(
        task_id='customers_datamart',
        conn_id=GREENPLUM_ID,
        sql=f'''
                DROP EXTERNAL TABLE IF EXISTS "{USER_PATH}".customers;

                CREATE EXTERNAL TABLE "{USER_PATH}".customers(
                    R_NAME TEXT, N_NAME TEXT, C_MKTSEGMENT TEXT, unique_customers_count BIGINT,
                    avg_acctbal FLOAT8, mean_acctbal FLOAT8, min_acctbal FLOAT8, max_acctbal FLOAT8)
        	    LOCATION ('pxf://de-project/{USER_PATH}/customers_report?PROFILE=s3:parquet&SERVER=default')
                ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
                ''',
        autocommit=True,
        split_statements=True,
        return_last=False)

    suppliers_submit_task = _build_submit_operator(task_id='suppliers',
                                                   application_file='spark-submit-suppliers.yaml',
                                                   link_dag=dag)

    suppliers_sensor_task = _build_sensor(task_id='suppliers', link_dag=dag)
    suppliers_datamart_task = SQLExecuteQueryOperator(
        task_id='suppliers_datamart',
        conn_id=GREENPLUM_ID,
        sql=f'''
                DROP EXTERNAL TABLE IF EXISTS "{USER_PATH}".suppliers;

                CREATE EXTERNAL TABLE "{USER_PATH}".suppliers(
                    R_NAME TEXT, N_NAME TEXT, unique_supplers_count BIGINT, avg_acctbal FLOAT8,
                    mean_acctbal FLOAT8, min_acctbal FLOAT8, max_acctbal FLOAT8)
                LOCATION ('pxf://de-project/{USER_PATH}/suppliers_report?PROFILE=s3:parquet&SERVER=default')
                ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
                ''',
        autocommit=True,
        split_statements=True,
        return_last=False)

    parts_submit_task = _build_submit_operator(task_id='parts',
                                               application_file='spark-submit-parts.yaml',
                                               link_dag=dag)

    parts_sensor_task = _build_sensor(task_id='parts', link_dag=dag)
    parts_datamart_task = SQLExecuteQueryOperator(
        task_id='parts_datamart',
        conn_id=GREENPLUM_ID,
        sql=f'''
                DROP EXTERNAL TABLE IF EXISTS "{USER_PATH}".parts;

                CREATE EXTERNAL TABLE "{USER_PATH}".parts(
                    N_NAME TEXT, P_TYPE TEXT, P_CONTAINER TEXT, parts_count BIGINT, avg_retailprice FLOAT8,
                    size BIGINT, mean_retailprice FLOAT8, min_retailprice FLOAT8, max_retailprice FLOAT8,
                    avg_supplycost FLOAT8, mean_supplycost FLOAT8, min_supplycost FLOAT8,
                    max_supplycost FLOAT8 )
                LOCATION ('pxf://de-project/{USER_PATH}/parts_report?PROFILE=s3:parquet&SERVER=default')
                ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
                ''',
        autocommit=True,
        split_statements=True,
        return_last=False)

    start >> lineitems_submit_task >> lineitems_sensor_task >> lineitems_datamart_task >> end
    start >> orders_submit_task >> orders_sensor_task >> orders_datamart_task >> end
    start >> customers_submit_task >> customers_sensor_task >> customers_datamart_task >> end
    start >> suppliers_submit_task >> suppliers_sensor_task >> suppliers_datamart_task >> end
    start >> parts_submit_task >> parts_sensor_task >> parts_datamart_task >> end
