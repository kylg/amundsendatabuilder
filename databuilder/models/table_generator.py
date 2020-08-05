from typing import Any, Dict, List, Union  # noqa: F401

from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable, NODE_KEY, \
    NODE_LABEL, RELATION_START_KEY, RELATION_START_LABEL, RELATION_END_KEY, \
    RELATION_END_LABEL, RELATION_TYPE, RELATION_REVERSE_TYPE

from databuilder.models.table_metadata import TableMetadata


class TableGenerator(Neo4jCsvSerializable):
    # type: (...) -> None
    """
    Hive table generator model.
    """
    LABEL = 'Generator'
    KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}/_generator'
    GENERATOR_TABLE_RELATION_TYPE = 'GENERATE'
    TABLE_GENERATOR_RELATION_TYPE = 'GENERATED_BY'

    def __init__(self,
                 db_name,  # type: str
                 schema,  # type: str
                 table_name,  # type: str
                 cluster,  # type: str
                 generator,  # type: str
                 generator_type='airflow',  # type: str
                 ):
        # type: (...) -> None
        self.db = db_name.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()

        self.cluster = cluster.lower() if cluster else 'gold'
        # generator is the generator id, such as dag_id of pipeline
        self.generator = generator
        self.generator_type = generator_type
        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self):
        # type: (...) -> Union[Dict[str, Any], None]
        # return the string representation of the data
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self):
        # type: (...) -> Union[Dict[str, Any], None]
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def get_generator_model_key(self):
        # type: (...) -> str
        return TableGenerator.KEY_FORMAT.format(db=self.db,
                                             cluster=self.cluster,
                                             schema=self.schema,
                                             tbl=self.table)

    def get_metadata_model_key(self):
        # type: (...) -> str
        return '{db}://{cluster}.{schema}/{table}'.format(db=self.db,
                                                          cluster=self.cluster,
                                                          schema=self.schema,
                                                          table=self.table)

    def create_nodes(self):
        # type: () -> List[Dict[str, Any]]
        """
        Create a list of Neo4j node records
        :return:
        """
        results = [{
            NODE_KEY: self.get_generator_model_key(),
            NODE_LABEL: TableGenerator.LABEL,
            'generator': self.generator,
            'generator_type': self.generator_type
        }]
        return results

    def create_relation(self):
        # type: () -> List[Dict[str, Any]]
        """
        Create a list of relation map between owner record with original hive table
        :return:
        """
        results = [{
            RELATION_START_KEY: self.get_generator_model_key(),
            RELATION_START_LABEL: TableGenerator.LABEL,
            RELATION_END_KEY: self.get_metadata_model_key(),
            RELATION_END_LABEL: TableMetadata.TABLE_NODE_LABEL,
            RELATION_TYPE: TableGenerator.GENERATOR_TABLE_RELATION_TYPE,
            RELATION_REVERSE_TYPE: TableGenerator.TABLE_GENERATOR_RELATION_TYPE
        }]
        return results

    def __repr__(self):
        # type: () -> str
        return 'TableGenerator({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.db,
                                                                  self.cluster,
                                                                  self.schema,
                                                                  self.table,
                                                                  self.generator)
