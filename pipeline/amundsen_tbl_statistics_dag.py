import logging
from datetime import datetime, timedelta
from airflow import DAG  # noqa
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator
import airflow.utils.dates
from airflow.operators.python_operator import PythonOperator
import os
import requests

os.environ["PYTHONIOENCODING"] = "utf-8"

LOGGER = logging.getLogger(__name__)

default_args = {
    'owner': 'amundsen',
    'start_date': airflow.utils.dates.days_ago(8),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'priority_weight': 10,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(days=2)
}

dag_id = 'amundsen_tbl_statistics'
dag = DAG(
    dag_id=dag_id, default_args=default_args,
    schedule_interval='@weekly', catchup=True)

csv_path = "dbfs:/tmp/platform/amundsen_sample_data"


def analyse_table_parameters():
    return {
        'today': '{{ ds }}',
        'csv_path': csv_path,
        'preview_rows':10
    }


def get_statistic_func(task_name, dag):
    task_params = {
        'existing_cluster_id': "1021-080656-hawks801",
        # 'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': "/platform/amundsen/amundsen_tbl_statistics",
            'base_parameters': analyse_table_parameters()
        },
        'libraries': []
    }
    load_task = DatabricksSubmitRunOperator(
        task_id=task_name,
        dag=dag,
        json=task_params)
    return load_task


preview_tmp_path = "/mnt/amundsen"


def get_download_csv_func(task_name, dag):
    csv_path_output = csv_path
    task = BashOperator(task_id=task_name, dag=dag,
                        bash_command="dbfs cp -r --overwrite {dbfsPath}/{date}/output {basePath}/{date}/".format(
                            dbfsPath=csv_path_output, basePath=preview_tmp_path, date='{{ ds }}'))
    return task


def send_csv_to_preview_service(**kwargs):
    """
    Launches data builder job that extracts table and column metadata from MySQL Hive metastore database,
    and publishes to Neo4j.
    @param today:
    @return:
    """
    LOGGER.info(kwargs)
    csv_path = kwargs["csv_path"]
    preview_service = kwargs["preview_service"]
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(csv_path) if isfile(join(csv_path, f)) and f.lower().endswith('.csv')]
    for f in onlyfiles:
        last_dot_index = f[:-4].rfind('.')
        db = f[:last_dot_index]
        tbl = f[last_dot_index+1:-4]
        files = {'file': open(join(csv_path, f),'rb')}
        data = {'database': db, 'dbschema': db, 'tableName': tbl}
        r = requests.post(preview_service, files=files, data=data)
        if r.status_code=='200':
            LOGGER.info('success sending file: {csv}'.format(csv=f))
        else:
            LOGGER.info('fail to send file: {csv}, code={code}, msg={msg}'.format(csv=f, code=r.status_code, msg=r.content))


preview_service = "http://10.216.61.58:31003/preview_data"

def create_send_csv_task(task_name, dag):
    op_kwargs = {"csv_path": preview_tmp_path, "preview_service":preview_service}
    task = PythonOperator(
        task_id=task_name,
        python_callable=send_csv_to_preview_service,
        provide_context=True,
        dag=dag,
        op_kwargs=op_kwargs
    )
    return task




statistic_task = get_statistic_func("analyse_table", dag)
download_preview_data = get_download_csv_func("download_preview_data", dag)
send_csv_task = create_send_csv_task("send_csv_to_preview_service", dag)
statistic_task >> download_preview_data >> send_csv_task
