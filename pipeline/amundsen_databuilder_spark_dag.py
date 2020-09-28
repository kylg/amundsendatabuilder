import logging
import textwrap
from datetime import timedelta
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyhocon import ConfigFactory
from airflow.models import Variable
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.extractor.sparksql_table_metadata_extractor import SparksqlTableMetadataExtractor
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.job.job import DefaultJob
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
import airflow.utils.dates
import requests
import os
import base64
import configparser
import zipfile
import io

# os.environ["PYTHONIOENCODING"] = "utf-8"

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

cluster_id = config.get('env', 'cluster_id')
# notebooks
metadata_notebooks = config.get('env', 'metadata_notebooks')
# databricks info
databricks_url_base = config.get('env', 'databricks_url_base')
databricks_token = Variable.get('amundsen_databricks_token_secret')
dbfs_metadata_export_path = config.get('env', 'dbfs_metadata_export_path')
dbfs_metadata_csv_path = config.get('env', 'dbfs_metadata_csv_path')
tbl_groups = 6
# local path
local_metadata_base = config.get('env', 'local_base') + '/table_metadata'
# NEO4J endpoints
neo4j_endpoint = config.get('env', 'neo4j_endpoint')
neo4j_user = Variable.get('amundsen_neo4j_user')
neo4j_password = Variable.get('amundsen_neo4j_password_secret')
# es endpoints
es_endpoint = config.get('env', 'es_endpoint')
es_url_prefix = config.get('env', 'es_url_prefix')
# airflow servers
default_airflow_server = config.get('env', 'default_airflow_server')
airflow_servers = [{"host": "localhost", "port": "3306", "user": "airflow", "password": "airflow",
                    "server": "http://10.216.61.36:8080/"}]

# Todo: user provides a list of schema for indexing
SUPPORTED_HIVE_SCHEMAS = ['hive']
# Global used in all Hive metastore queries.
# String format - ('schema1', schema2', .... 'schemaN')
SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_HIVE_SCHEMAS))

default_args = {
    'owner': 'amundsen',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'priority_weight': 10,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=120)
}

dag_id = 'amundsen_databuilder'
dag = DAG(
    dag_id=dag_id, default_args=default_args,
    schedule_interval='@daily', catchup=False)

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


# Todo: user needs to modify and provide a hivemetastore connection string
def connection_string():
    return 'hivemetastore.connection'


def collect_tblinfo_task_parameters():
    return {
        'today': '{{ ds }}',
        'export_path': dbfs_metadata_export_path,
        'export_csv_path': dbfs_metadata_csv_path,
        'tbl_groups': tbl_groups
    }


def get_collect_tblinfo_func(task_name, dag):
    task_params = {
        # 'existing_cluster_id': "1021-080656-hawks801",
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': metadata_notebooks,
            'base_parameters': collect_tblinfo_task_parameters()
        },
        'libraries': []
    }
    load_task = DatabricksSubmitRunOperator(
        task_id=task_name,
        dag=dag,
        json=task_params)
    return load_task


def download_csv_0(file_no, exec_day):
    parent_path = '{base_path}/csv/'.format(base_path=local_metadata_base)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path)
    else:
        from os import listdir
        for f in listdir(parent_path):
            print("list files: " + parent_path + f)

    dbfs_path = dbfs_metadata_csv_path[5:] + "/" + exec_day
    list_url = "{dbfs_base}/api/2.0/dbfs/list?path={file_path}".format(dbfs_base=databricks_url_base,
                                                                       file_path=dbfs_path)
    headers = {'Authorization': 'Bearer {token}'.format(token=databricks_token)}
    files = list_folder(list_url, headers=headers)
    target_filename = '{data_day}-{file_no}.csv'.format(data_day=exec_day, file_no=file_no)
    for f in files:
        path = f['path']
        if not path.endswith(target_filename):
            continue
        read_url = "{dbfs_base}/api/2.0/dbfs/read?path={file_path}".format(dbfs_base=databricks_url_base,
                                                                           file_path=path)
        # get the file size and
        file_size = f['file_size']
        file_name = path[path.rfind('/') + 1:]
        local_file = "{parent_path}/{file_name}".format(parent_path=parent_path, file_name=file_name)
        read_one_file(read_url, local_file, file_size, headers)
        return local_file


def list_folder(list_url, headers):
    list_rsp = requests.get(list_url, headers=headers)
    if list_rsp.status_code == 200:
        return list_rsp.json()['files']
    else:
        logging.error(
            "error to list path: status_code={code}, msg={msg}".format(code=list_rsp.status_code, msg=list_rsp.text))
        return []


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


def create_neo4j(**kwargs):
    """
    Launches data builder job that extracts table and column metadata from MySQL Hive metastore database,
    and publishes to Neo4j.
    @param today:
    @return:
    """
    LOGGER.info(kwargs)
    exec_day = kwargs["ds"]
    file_no = kwargs["file_no"]
    local_file = download_csv_0(file_no, exec_day)
    if local_file is None:
        logging.info("no local csv file to write to neo4j")
        return

    # Adding to where clause to scope schema, filter out temp tables which start with numbers and views
    where_clause_suffix = textwrap.dedent("""
        WHERE d.NAME IN {schemas}
        AND t.TBL_NAME NOT REGEXP '^[0-9]+'
        AND t.TBL_TYPE IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
    """).format(schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)
    LOGGER.info("======exec_day=" + str(exec_day))
    node_files_folder = '{basePath}/nodes_{file_no}/'.format(basePath=local_metadata_base, file_no=file_no)
    relationship_files_folder = '{basePath}/relationships_{file_no}/'.format(basePath=local_metadata_base,
                                                                             file_no=file_no)

    job_config = ConfigFactory.from_dict({
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_CREATE_ONLY_NODES):
            [DESCRIPTION_NODE_LABEL],
        'extractor.sparksql_table_metadata.csv_file_path': local_file,
        'extractor.sparksql_table_metadata.airflow_servers': airflow_servers,
        'extractor.sparksql_table_metadata.default_airflow_server': default_airflow_server,
        'publisher.neo4j.job_publish_tag': str(exec_day)
    })

    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=SparksqlTableMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_neo4j_task(task_name, dag, file_no):
    op_kwargs = {"file_no": file_no}
    task = PythonOperator(
        task_id=task_name + "_" + str(file_no),
        python_callable=create_neo4j,
        provide_context=True,
        dag=dag,
        op_kwargs=op_kwargs
    )
    return task


def create_elasticsearch(**kwargs):
    LOGGER.info(kwargs)
    exec_day = kwargs["ds"]
    LOGGER.info("======exec_day=" + str(exec_day))
    es_temp_file = '{basePath}/es/{exec_day}.json'.format(basePath=local_metadata_base, exec_day=exec_day)
    es = Elasticsearch(hosts=[es_endpoint], url_prefix=es_url_prefix, scheme="https", ca_certs=False,
                       verify_certs=False)
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables_{exec_day}'.format(exec_day=exec_day)
    # related to mapping type from /databuilder/publisher/elasticsearch_publisher.py#L38
    # elasticsearch_new_index_key_type = 'table'
    elasticsearch_doc_type = 'table'
    # alias for Elasticsearch used in amundsensearchlibrary/search_service/config.py as an index
    elasticsearch_index_alias = 'table_search_index'

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY):
            neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY):
            'databuilder.models.table_elasticsearch_document.TableESDocument',
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER):
            neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW):
            neo4j_password,
        # 'extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY):
        #     Neo4jSearchDataExtractor.DEFAULT_NEO4J_TABLE_CYPHER_QUERY,

        'extractor.search_data.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            exec_day,

        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            es_temp_file,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY):
            'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            es_temp_file,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY):
            'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            es,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_doc_type,
        # 'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY):
        #     DASHBOARD_ELASTICSEARCH_INDEX_MAPPING,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias,
    })

    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=Neo4jSearchDataExtractor(),
                                      loader=FSElasticsearchJSONLoader()),
                     publisher=ElasticsearchPublisher())
    job.launch()


def create_elasticsearch_task(task_name, dag):
    task = PythonOperator(
        task_id=task_name,
        python_callable=create_elasticsearch,
        provide_context=True,
        dag=dag
    )
    return task


collect_tblinfo_task = get_collect_tblinfo_func("collect_tblinfo", dag)
create_metadata_task_0 = create_neo4j_task('create_metadata', dag, 0)
create_metadata_task_1 = create_neo4j_task('create_metadata', dag, 1)
create_metadata_task_2 = create_neo4j_task('create_metadata', dag, 2)
create_metadata_task_3 = create_neo4j_task('create_metadata', dag, 3)
create_metadata_task_4 = create_neo4j_task('create_metadata', dag, 4)
create_metadata_task_5 = create_neo4j_task('create_metadata', dag, 5)
create_index_task = create_elasticsearch_task('create_index', dag)
collect_tblinfo_task >> create_metadata_task_0 >> create_metadata_task_1 >> create_metadata_task_2 >> create_metadata_task_3 >> create_metadata_task_4 >> create_metadata_task_5 >> create_index_task
