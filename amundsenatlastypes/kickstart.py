import re

from atlasclient.utils import parse_table_qualified_name
from requests import Timeout

from amundsenatlastypes.client import driver


# noinspection SpellCheckingInspection
class KickstartExistingData:

    def create_entities(self, entities_to_create):
        try:
            driver.entity_bulk.create(data={"entities": entities_to_create})
        except Timeout as ex:
            # Try one more time in case of Timeout error!!
            print(f'ReadTimeout : {ex}')
            driver.entity_bulk.create(data={"entities": entities_to_create})

    # TODO: To be moved to pyatlasclient package
    # noinspection PyMethodMayBeStatic
    def parse_column_qualified_name(self, qualified_name):
        qn_regex = re.compile(r"""
            ^(?P<db_name>.*?)\.(?P<table_name>.*)\.(?P<column_name>.*)@(?P<cluster_name>.*?)$
            """, re.X)

        def apply_qn_regex(name, column_qn_regex):
            return column_qn_regex.match(name)

        _regex_result = apply_qn_regex(qualified_name, qn_regex)

        if not _regex_result:
            qn_regex = re.compile(r"""
            ^(?P<table_name>.*?)\.(?P<column_name>.*)@(?P<cluster_name>.*?)$
            """, re.X)
            _regex_result = apply_qn_regex(qualified_name, qn_regex)

        if not _regex_result:
            qn_regex = re.compile(r"""
            ^(?P<column_name>.*)@(?P<cluster_name>.*?)$
            """, re.X)
            _regex_result = apply_qn_regex(qualified_name, qn_regex)

        if not _regex_result:
            qn_regex = re.compile(r"""
            ^(?P<column_name>.*)$
            """, re.X)
            _regex_result = apply_qn_regex(qualified_name, qn_regex)

        _regex_result = _regex_result.groupdict()

        qn_dict = {
            'column_name': _regex_result.get('column_name', qualified_name),
            'table_name': _regex_result.get('table_name', "default"),
            'db_name': _regex_result.get('db_name', "default"),
            'cluster_name': _regex_result.get('cluster_name', "default"),
        }

        return qn_dict

    # noinspection PyMethodMayBeStatic
    def get_column_metadata(self, column_entity):
        column_qn = column_entity.attributes.get("qualifiedName")
        column_info = self.parse_column_qualified_name(column_qn)
        column_guid = column_entity.guid

        column_metadata_qn = f'{column_info["db_name"]}.' \
                             f'{column_info["table_name"]}.' \
                             f'{column_info["column_name"]}.' \
                             f'metadata@{column_info["cluster_name"]}'

        metadata_entity = {'typeName': 'column_metadata',
                           'attributes': {'qualifiedName': column_metadata_qn,
                                          'popularityScore': 0,
                                          'column': {'guid': column_guid}}
                           }
        return metadata_entity

    def create_columns_metadata(self):
        limit = 50
        offset = 0
        results = True
        count_query = {'query': "from Column where  __state = 'ACTIVE' select count()"}
        count_results = list(driver.search_dsl(**count_query))[0]
        count_value = count_results._data['attributes']['values'][0][0]

        while results:
            params = {'typeName': 'Column',
                      'attributes': ['metadata'],
                      'limit': limit,
                      'offset': offset
                      }
            search_results = driver.search_basic.create(data=params)

            entities_to_create = list()
            for entity in search_results.entities:
                meta_guid = entity.attributes.get("metadata", {}).get("guid")
                meta_entity = search_results.referredEntities and search_results.referredEntities.get(meta_guid)
                if not meta_guid or (meta_guid and meta_entity.get("status") != "ACTIVE"):
                    entities_to_create.append(self.get_column_metadata(entity))

            if entities_to_create:
                self.create_entities(entities_to_create)

            if count_value > 0 and offset <= count_value:
                offset += 50
            else:
                results = False

    # noinspection PyMethodMayBeStatic
    def get_table_metadata(self, table_entity):
        """
        database.table.metadata@cluster
        """
        table_qn = table_entity.attributes.get("qualifiedName")
        table_info = parse_table_qualified_name(table_qn)
        table_guid = table_entity.guid

        metadata_qn = f'{table_info["db_name"]}.{table_info["table_name"]}.metadata@{table_info["cluster_name"]}'

        metadata_entity = {'typeName': 'table_metadata',
                           'attributes': {'qualifiedName': metadata_qn,
                                          'popularityScore': 0,
                                          'table': {'guid': table_guid}}
                           }
        return metadata_entity

    # noinspection PyMethodMayBeStatic
    def create_table_metadata(self):
        limit = 50
        offset = 0
        results = True
        count_query = {'query': "from Table where  __state = 'ACTIVE' select count()"}
        count_results = list(driver.search_dsl(**count_query))[0]
        count_value = count_results._data['attributes']['values'][0][0]

        while results:
            params = {'typeName': 'Table',
                      'attributes': ['metadata'],
                      'limit': limit,
                      'offset': offset
                      }
            search_results = driver.search_basic.create(data=params)

            entities_to_create = list()
            for entity in search_results.entities:
                meta_guid = entity.attributes.get("metadata", {}).get("guid")
                meta_entity = search_results.referredEntities and search_results.referredEntities.get(meta_guid)
                if not meta_guid or (meta_guid and meta_entity.get("status") != "ACTIVE"):
                    entities_to_create.append(self.get_table_metadata(entity))

            if entities_to_create:
                self.create_entities(entities_to_create)

            if count_value > 0 and offset <= count_value:
                offset += 50
            else:
                results = False

    def initiate_existing_data(self):
        print("Creating Table Metadata Entities")
        self.create_table_metadata()

        print("Creating Column Metadata Entities")
        self.create_columns_metadata()
