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

import os
os.environ["PYTHONIOENCODING"] = "utf-8"


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

dag_id = 'amundsen_analyse_table'
dag = DAG(
    dag_id=dag_id, default_args=default_args,
    schedule_interval='@weekly', catchup=False)



def analyse_table_parameters():
    return {
        'today': '{{ ds }}',
        'log_path': "dbfs:/tmp/platform/amundsen_log"
    }


def get_collect_tblinfo_func(task_name, dag):
    task_params = {
        'existing_cluster_id': "1021-080656-hawks801",
        # 'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': "/DigitalMarketing/test_ci/amundsen_analyse_table",
            'base_parameters': analyse_table_parameters()
        },
        'libraries': []
    }
    load_task = DatabricksSubmitRunOperator(
        task_id=task_name,
        dag=dag,
        json=task_params)
    return load_task


collect_tblinfo_task = get_collect_tblinfo_func("analyse_table", dag)
