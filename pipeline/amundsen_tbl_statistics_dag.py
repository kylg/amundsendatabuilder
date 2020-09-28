import logging
from datetime import timedelta
import configparser
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
import airflow.utils.dates
from airflow.operators.python_operator import PythonOperator
import os
import requests
from airflow.models import Variable
import base64
from shutil import rmtree
import zipfile
import io

LOGGER = logging.getLogger(__name__)


def load_config(file_name, config):
    current_file = os.path.abspath(os.path.dirname(__file__))
    if current_file.endswith(".zip"):
        LOGGER.info("===========env.properties: " + current_file)
        zf = zipfile.ZipFile(current_file)
        LOGGER.info(zf.namelist())
        zf_config = zf.open(file_name, "r")
        zf_config = io.TextIOWrapper(zf_config)
        config.read_file(zf_config)
    else:
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file_name)
        LOGGER.info("===========env.properties: " + path)
        config.read([path])


config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
load_config('env.properties', config)

# notebooks
statistics_notebooks= config.get('env', 'statistics_notebooks')
# databricks info
databricks_token = Variable.get('amundsen_databricks_token_secret')
cluster_id = config.get('env', 'cluster_id')
databricks_url_base = config.get('env', 'databricks_url_base')
dbfs_csv_path = config.get('env', 'dbfs_preview_csv_path')
preview_service_url = config.get('env', 'preview_service_url')
local_csv_path = config.get('env', 'local_base')+'/preview'

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

new_cluster = {
    'spark_version': '5.2.x-scala2.11',
    'node_type_id': 'Standard_DS5_v2',
    'spark_conf': {
        'dfs.adls.oauth2.client.id': Variable.get('adls_oauth2_client_id_secret'),
        'dfs.adls.oauth2.refresh.url': Variable.get('adls_oauth2_url'),
        'dfs.adls.oauth2.credential': Variable.get('adls_oauth2_credential_secret'),
        'dfs.adls.oauth2.access.token.provider.type': Variable.get('dfs.adls.oauth2.access.token.provider.type'),
        'dfs.adls.oauth2.access.token.provider': Variable.get('dfs.adls.oauth2.access.token.provider')
    },
    'num_workers': 2,
    'custom_tags': {
        'pipeline_name': dag_id,
        'project_name': 'Amundsen',
        'subproject_name': 'Amundsen Databuilder',
        'description': 'amundsen databuilder'
    }
}

dag = DAG(
    dag_id=dag_id, default_args=default_args,
    schedule_interval='@weekly', catchup=False)


def analyse_table_parameters():
    return {
        'today': '{{ ds }}',
        'csv_path': dbfs_csv_path,
        'preview_rows': 10
    }


def get_statistic_func(task_name, dag):
    task_params = {
        # 'existing_cluster_id': "1021-080656-hawks801",
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': statistics_notebooks,
            'base_parameters': analyse_table_parameters()
        },
        'libraries': []
    }
    load_task = DatabricksSubmitRunOperator(
        task_id=task_name,
        dag=dag,
        json=task_params)
    return load_task


def download_csv(dbfs_csv_base, local_csv_base, date):
    local_path = '{base_path}/{date}/'.format(base_path=local_csv_base, date=date)
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    else:
        from os import listdir
        for f in listdir(local_path):
            print("list files: " + local_path + f)

    dbfs_path = dbfs_csv_base[5:] + "/" + date + "/output"
    list_url = "{dbfs_base}/api/2.0/dbfs/list?path={file_path}".format(dbfs_base=databricks_url_base,
                                                                       file_path=dbfs_path)
    headers = {'Authorization': 'Bearer {token}'.format(token=databricks_token)}
    files = list_folder(list_url, headers=headers)
    for f in files:
        path = f['path']
        read_url = "{dbfs_base}/api/2.0/dbfs/read?path={file_path}".format(dbfs_base=databricks_url_base,
                                                                           file_path=path)
        file_size = f['file_size']
        file_name = path[path.rfind('/') + 1:]
        local_file = "{parent_path}/{file_name}".format(parent_path=local_path, file_name=file_name)
        read_one_file(read_url, local_file, file_size, headers)


def read_one_file(read_url, local_file, file_size, headers):
    logging.info("write csv to local: {local_file}".format(local_file=local_file))
    offset = 0
    f = open(local_file, 'wb')
    batch_size = 1048576
    while offset < file_size:
        full_url = '{read_url}&offset={offset}&length={length}'.format(read_url=read_url, offset=offset,
                                                                       length=batch_size)
        logging.info("full_url: {full_url}".format(full_url=full_url))
        print(full_url)
        read_rsp = requests.get(full_url, headers=headers)
        if read_rsp.status_code == 200:
            j = read_rsp.json()
            if j['bytes_read'] > 0:
                bs = base64.b64decode(j['data'])
                f.write(bs)
                offset = offset + j['bytes_read']
            else:
                logging.error("bytes_read is 0, so break: {read_url}".format(read_url=read_url))
                break
        else:
            logging.error("error to read csv file: status_code={code}, msg={msg}".format(code=read_rsp.status_code,
                                                                                         msg=read_rsp.text))
            break
    f.close()


def list_folder(list_url, headers):
    list_rsp = requests.get(list_url, headers=headers)
    if list_rsp.status_code == 200:
        return list_rsp.json()['files']
    else:
        logging.error(
            "error to list path: status_code={code}, msg={msg}".format(code=list_rsp.status_code, msg=list_rsp.text))
        return []


def send_csv_to_preview_service(**kwargs):
    """
    Launches data builder job that extracts table and column metadata from MySQL Hive metastore database,
    and publishes to Neo4j.
    @param today:
    @return:
    """
    LOGGER.info(kwargs)
    dbfs_csv_base = kwargs["dbfs_csv_path"]
    local_csv_base = kwargs["local_csv_path"]
    today = kwargs['today']
    preview_service = kwargs["preview_service"]
    # down load csv files to local disk
    download_csv(dbfs_csv_base, local_csv_base, today)
    from os import listdir
    from os.path import isfile, join
    csv_path = '{local_csv_base}/{date}'.format(local_csv_base=local_csv_base, date=today)
    only_files = [f for f in listdir(csv_path) if isfile(join(csv_path, f)) and f.lower().endswith('.csv')]
    for f in only_files:
        LOGGER.info("begin to send file {f}".format(f=f))
        last_dot_index = f[:-4].rfind('.')
        db = f[:last_dot_index]
        tbl = f[last_dot_index + 1:-4]
        files = {'file': open(join(csv_path, f), 'rb')}
        data = {'database': db, 'schema': db, 'tableName': tbl}
        LOGGER.info('database={database}, schema={schema}, tbl={tbl}'.format(database=db, schema=db, tbl=tbl))
        r = requests.put(preview_service, files=files, data=data, verify=False)
        if r.status_code == '200':
            LOGGER.info('success sending file: {csv}'.format(csv=f))
        else:
            LOGGER.info(
                'fail to send file: {csv}, code={code}, msg={msg}'.format(csv=f, code=r.status_code, msg=r.content))
    rmtree(local_csv_base)


def create_send_csv_task(task_name, dag):
    op_kwargs = {"dbfs_csv_path": dbfs_csv_path, "local_csv_path": local_csv_path, 'today': '{{ ds }}',
                 "preview_service": preview_service_url}
    task = PythonOperator(
        task_id=task_name,
        python_callable=send_csv_to_preview_service,
        provide_context=True,
        dag=dag,
        op_kwargs=op_kwargs
    )
    return task


statistic_task = get_statistic_func("analyse_table", dag)
send_csv_task = create_send_csv_task("send_csv_to_preview_service", dag)
statistic_task >> send_csv_task
