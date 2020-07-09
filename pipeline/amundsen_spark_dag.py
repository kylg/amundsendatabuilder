import logging
import textwrap
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from airflow import DAG  # noqa
from airflow.operators.python_operator import PythonOperator  # noqa
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.extractor.sparksql_table_metadata_extractor import SparksqlTableMetadataExtractor
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator
import airflow.utils.dates

LOGGER = logging.getLogger(__name__)

default_args = {
    'owner': 'amundsen',
    'start_date': airflow.utils.dates.days_ago(2),
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

# NEO4J cluster endpoints
neo4j_endpoint = 'bolt://10.216.61.59:31010'

neo4j_user = 'neo4j'
neo4j_password = 'test'

ES_HOST = '10.216.61.59'
ES_PORT = 31020

# Todo: user provides a list of schema for indexing
SUPPORTED_HIVE_SCHEMAS = ['hive']
# Global used in all Hive metastore queries.
# String format - ('schema1', schema2', .... 'schemaN')
SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_HIVE_SCHEMAS))

EXPORT_PATH = "dbfs:/tmp/pf/tbl_def/"
DBFS_CSV_PATH = "dbfs:/tmp/pf/tbl_def_csv"

tmp_folder = '/var/tmp/amundsen/table_metadata'


# Todo: user needs to modify and provide a hivemetastore connection string
def connection_string():
    return 'hivemetastore.connection'


def collect_tblinfo_task_parameters():
    return {
        'today': '{{ ds }}',
        'export_path': EXPORT_PATH,
        'export_dbfs_path': DBFS_CSV_PATH
    }


def get_collect_tblinfo_func(task_name, dag):
    task_params = {
        'existing_cluster_id': "1021-080656-hawks801",
        # 'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': "/DigitalMarketing/test_ci/amundsen_spark_extractor",
            'base_parameters': collect_tblinfo_task_parameters()
        },
        'libraries': []
    }
    load_task = DatabricksSubmitRunOperator(
        task_id=task_name,
        dag=dag,
        json=task_params)
    return load_task


def get_make_localdirs_func(task_name, dag):
    task = BashOperator(task_id=task_name, dag=dag, bash_command="mkdir -p {basePath}/csv".format(basePath=tmp_folder))
    return task


def get_download_csv_func(task_name, dag):
    task = BashOperator(task_id=task_name, dag=dag,
                        bash_command="dbfs cp --overwrite {dbfsPath}/{csvFile}.csv {basePath}/csv/".format(
                            dbfsPath=DBFS_CSV_PATH, csvFile='{{ ds }}', basePath=tmp_folder))
    return task


def create_neo4j(**kwargs):
    """
    Launches data builder job that extracts table and column metadata from MySQL Hive metastore database,
    and publishes to Neo4j.
    @param today:
    @return:
    """
    LOGGER.info(kwargs)
    exec_day = kwargs["ds"]

    # Adding to where clause to scope schema, filter out temp tables which start with numbers and views
    where_clause_suffix = textwrap.dedent("""
        WHERE d.NAME IN {schemas}
        AND t.TBL_NAME NOT REGEXP '^[0-9]+'
        AND t.TBL_TYPE IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
    """).format(schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)
    LOGGER.info("======exec_day=" + str(exec_day))
    node_files_folder = '{basePath}/nodes/'.format(basePath=tmp_folder)
    relationship_files_folder = '{basePath}/relationships/'.format(basePath=tmp_folder)

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
        'extractor.sparksql_table_metadata.csv_file_path': "{basePath}/csv/{data_day}.csv".format(basePath=tmp_folder,
                                                                                                  data_day=str(
                                                                                                      exec_day)),
        'publisher.neo4j.job_publish_tag': str(exec_day)
    })

    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=SparksqlTableMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_neo4j_task(task_name, dag):
    task = PythonOperator(
        task_id=task_name,
        python_callable=create_neo4j,
        provide_context=True,
        dag=dag
    )
    return task


def create_elasticsearch(**kwargs):
    LOGGER.info(kwargs)
    exec_day = kwargs["ds"]
    LOGGER.info("======exec_day=" + str(exec_day))
    es_temp_file = '{basePath}/es/{exec_day}.json'.format(basePath=tmp_folder, exec_day=exec_day)
    es = Elasticsearch([
        {'host': ES_HOST, "port": ES_PORT},
    ])
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
mk_localdirs_task = get_make_localdirs_func("make_localdirs", dag)
download_csv_task = get_download_csv_func("download_csv", dag)
create_metadata_task = create_neo4j_task('create_metadata', dag)
create_index_task = create_elasticsearch_task('create_index', dag)
collect_tblinfo_task >> mk_localdirs_task >> download_csv_task >> create_metadata_task >> create_index_task
