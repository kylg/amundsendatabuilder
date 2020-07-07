import logging
from collections import namedtuple
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from itertools import groupby


TableKey = namedtuple('TableKey', ['dbName', 'tblName'])

LOGGER = logging.getLogger(__name__)


class SparksqlTableMetadataExtractor(Extractor):

    def init(self, conf):
        csv_file = conf.get_string("csv_file_path")
        import csv
        with open(csv_file) as f:
            reader = csv.reader(f)
            data = list(reader)

        print(data)
        if(len(data)<1):
            LOGGER.info('no data from csv: {}'.format(csv_file))
            return

        self.header = data[0]
        self.posDict = {}
        for i in range(len(self.header)):
            self.posDict[self.header[i]] = i

        self.content = data[1:]
        LOGGER.info("header========"+",".join(self.header))
        LOGGER.info("content length========"+str(len(self.content)))
        self._extract_iter = None  # type: Union[None, Iterator]


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
        for key, group in groupby(iter(self.content), self._get_table_key):
            columns = []
            partitionKeys = []
            for row in group:
                last_row = row
                if(row[self.posDict["isPartition"]] != 'false'):
                    partitionKeys.append(row[self.posDict["colName"]])
                columns.append(ColumnMetadata(row[self.posDict["colName"]], row[self.posDict["colDesc"]],
                                              row[self.posDict["colType"]], row[self.posDict["colSortOrder"]]))

            is_view = last_row[self.posDict["isView"]] == 'true'
            partitionStr = ""
            if len(partitionKeys)>0:
                partitionStr = ",".join(partitionKeys)
            yield TableMetadata(last_row[self.posDict['dbName']], 'northeurope',
                                last_row[self.posDict['dbName']],
                                last_row[self.posDict['tblName']],
                                last_row[self.posDict['tblDesc']],
                                columns,
                                is_view=is_view,
                                partitionKeys=partitionStr,
                                tblLocation=last_row[self.posDict["tblLocation"]]
                                )

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(dbName=row[self.posDict["dbName"]], tblName=row[self.posDict["tblName"]])

        return None
