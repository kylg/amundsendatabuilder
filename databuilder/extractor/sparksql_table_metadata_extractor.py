import logging
from collections import namedtuple
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_last_updated import TableLastUpdated
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from databuilder.models.table_stats import TableColumnStats
from databuilder.models.watermark import Watermark
from databuilder.models.table_source import TableSource
from databuilder.models.table_generator import TableGenerator
from itertools import groupby
import mysql.connector

TableKey = namedtuple('TableKey', ['dbName', 'tblName'])

LOGGER = logging.getLogger(__name__)


class DagDef:
    def __init__(self,
                 dag_id,  # type: str
                 is_paused,  # type: bool
                 file_loc,  # type: str
                 url
                 ):
        self.dag_id = dag_id
        self.is_paused = is_paused
        self.file_loc = file_loc
        self.url = url

class SparksqlTableMetadataExtractor(Extractor):

    def init(self, conf):
        csv_file = conf.get_string("csv_file_path")
        self.airflow_servers = conf.get("airflow_servers")
        self.default_airflow_server = conf.get("default_airflow_server")
        self._init_airflow_ids()
        import csv
        with open(csv_file) as f:
            reader = csv.reader(f)
            data = list(reader)

        print(data)
        if (len(data) < 1):
            LOGGER.info('no data from csv: {}'.format(csv_file))
            return

        self.header = data[0]
        self.pos_dict = {}
        for i in range(len(self.header)):
            self.pos_dict[self.header[i]] = i

        self.content = data[1:]
        LOGGER.info("header========" + ",".join(self.header))
        LOGGER.info("content length========" + str(len(self.content)))
        self._extract_iter = None  # type: Union[None, Iterator]

    def _init_airflow_ids(self):
        self.dag_defs = {}
        for db_info in self.airflow_servers:
            db = mysql.connector.connect(
                host=db_info["host"],
                port=db_info["port"],
                user=db_info["user"],
                password=db_info["password"],
                database="airflow"
            )
            cursor = db.cursor()
            cursor.execute('SELECT dag_id,is_active,fileloc FROM dag')
            result = cursor.fetchall()
            for row in result:
                dag_id = row[0]
                if dag_id in self.dag_defs.keys():
                    # if a dag_id reside in multiple airflow, choose the active one
                    if not row[1]:
                        continue
                url = "{server}/tree?dag_id={dag_id}".format(server=db_info["server"], dag_id=dag_id)
                self.dag_defs[dag_id] = DagDef(dag_id, row[1], row[2], url)

    def get_airflow_link(self, dag_id):
        if dag_id in self.dag_defs.keys():
            return self.dag_defs[dag_id].url
        return "{server}/tree?dag_id={dag_id}".format(server=self.default_airflow_server, dag_id=dag_id)

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.sparksql_table_metadata'

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        cluster = 'northeurope'
        for key, group in groupby(iter(self.content), self._get_table_key):
            columns = []
            partitionKeys = []
            colStats = []
            for row in group:
                last_row = row
                if row[self.pos_dict["isPartition"]] != 'false':
                    partitionKeys.append(row[self.pos_dict["colName"]])
                columns.append(ColumnMetadata(row[self.pos_dict["colName"]], row[self.pos_dict["colDesc"]],
                                              row[self.pos_dict["colType"]], row[self.pos_dict["colSortOrder"]]))
                self._create_stats(row, cluster, colStats)

            is_view = last_row[self.pos_dict["isView"]] == 'true'
            partitionStr = ""
            if len(partitionKeys) > 0:
                partitionStr = ",".join(partitionKeys)
            LOGGER.debug("partitionStr=" + partitionStr)
            dbName = last_row[self.pos_dict['dbName']]
            tblName = last_row[self.pos_dict['tblName']]

            yield TableMetadata(dbName, cluster, dbName, tblName,
                                last_row[self.pos_dict['tblDesc']],
                                columns,
                                is_view=is_view,
                                partitionKeys=partitionStr,
                                tblLocation=last_row[self.pos_dict["tblLocation"]])

            if last_row[self.pos_dict['lastUpdateTime']] > 0:
                yield TableLastUpdated(tblName,
                                       last_row[self.pos_dict['lastUpdateTime']],
                                       dbName, dbName, cluster)
            if last_row[self.pos_dict['p0Name']] is not None and last_row[self.pos_dict['p0Name']] != '':
                yield Watermark(last_row[self.pos_dict['p0Time']], dbName, dbName, tblName,
                                last_row[self.pos_dict['p0Name']], 'low_watermark', cluster)
                yield Watermark(last_row[self.pos_dict['p1Time']], dbName, dbName, tblName,
                                last_row[self.pos_dict['p1Name']], 'high_watermark', cluster)

            source = self._create_source(dbName, dbName, tblName, cluster, last_row[self.pos_dict['source']])
            if source:
                yield source
            generator = self._create_generator(dbName, dbName, tblName, cluster, last_row[self.pos_dict['pipeline']])
            if generator:
                yield generator

            for colStat in colStats:
                yield colStat

    def _create_stats(self, row, cluster, colStats):
        # nullCount:BigInt, distinctCount:BigInt, max:String, min:String, avgLen:Any, maxLen:Any
        dbName = row[self.pos_dict["dbName"]]
        tblName = row[self.pos_dict['tblName']]
        colName = row[self.pos_dict["colName"]]
        nullCount = row[self.pos_dict["nullCount"]]
        distinctCount = row[self.pos_dict["distinctCount"]]
        max = row[self.pos_dict["max"]]
        min = row[self.pos_dict["min"]]
        avgLen = row[self.pos_dict["avgLen"]]
        maxLen = row[self.pos_dict["maxLen"]]
        LOGGER.info("stat===" + str(nullCount) + "," + str(distinctCount) + "," + str(max) + "," + str(min) + "," + str(
            avgLen) + "," + str(maxLen))
        if nullCount is not None and len(str(nullCount).strip()) > 0:
            colStats.append(
                TableColumnStats(tblName, colName, "nullCount", str(nullCount), 0, 0, dbName, cluster, dbName))
        if distinctCount is not None and len(str(distinctCount).strip()) > 0:
            colStats.append(
                TableColumnStats(tblName, colName, "distinctCount", str(distinctCount), 0, 0, dbName, cluster, dbName))
        if max is not None and len(str(max).strip()) > 0:
            colStats.append(TableColumnStats(tblName, colName, "max", str(max), 0, 0, dbName, cluster, dbName))
        if min is not None and len(str(min).strip()) > 0:
            colStats.append(TableColumnStats(tblName, colName, "min", str(min), 0, 0, dbName, cluster, dbName))
        if avgLen is not None and len(str(avgLen).strip()) > 0:
            colStats.append(TableColumnStats(tblName, colName, "avgLen", str(avgLen), 0, 0, dbName, cluster, dbName))
        if maxLen is not None and len(str(maxLen).strip()) > 0:
            colStats.append(TableColumnStats(tblName, colName, "maxLen", str(maxLen), 0, 0, dbName, cluster, dbName))

    def _create_source(self, db_name, schema, table_name, cluster, source):
        if source:
            if source.lower().index("bitbucket")>=0:
                source_type = 'bitbucket'
            elif source.lower().index("github")>=0:
                source_type = 'github'
            else:
                source_type = 'unknown'
            return TableSource(db_name, schema, table_name, cluster, source, source_type)
        else:
            return None

    def _create_generator(self, db_name, schema, table_name, cluster, pipeline):
        if pipeline:
            url = self.get_airflow_link(pipeline)
            if not url:
                url = pipeline
            return TableGenerator(db_name, schema, table_name, cluster, url, 'airflow')
        else:
            return None

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(dbName=row[self.pos_dict["dbName"]], tblName=row[self.pos_dict["tblName"]])

        return None
