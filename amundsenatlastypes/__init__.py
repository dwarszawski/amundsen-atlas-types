import json
import os

# noinspection PyPackageRequirements
from atlasclient.client import Atlas
from atlasclient.utils import parse_table_qualified_name
# noinspection PyPackageRequirements
from atlasclient.exceptions import Conflict, HttpError
from requests import Timeout

from .types import *


class AtlasClient:
    host = os.environ.get('ATLAS_HOST', 'localhost')
    port = os.environ.get('ATLAS_PORT', 21000)
    user = os.environ.get('ATLAS_USERNAME', 'admin')
    password = os.environ.get('ATLAS_PASSWORD', 'admin')
    timeout = os.environ.get('ATLAS_REQUEST_TIMEOUT', 10)

    def driver(self):
        return Atlas(host=self.host,
                     port=self.port,
                     username=self.user,
                     password=self.password,
                     timeout=self.timeout)


# noinspection PyMethodMayBeStatic
class Initializer:
    def assign_subtypes(self, ends_with="_table", super_type="Table"):
        print(f'\nAssigning {super_type} entity to all the subtypes entity definitions')
        entities_to_update = []
        for t in self.driver.typedefs:
            for e in t.entityDefs:
                if e.name.endswith(ends_with):  # Assign new entity to all the tables in atlas
                    print(f'Assigning {e.name} as a subtype of {super_type}')
                    super_types = e.superTypes  # Get a property first to inflate the relational objects
                    ent_dict = e._data
                    ent_dict["superTypes"] = super_types
                    ent_dict["superTypes"].append(super_type)
                    entities_to_update.append(ent_dict)

        typedef_dict = {
            "entityDefs": entities_to_update
        }
        self.driver.typedefs.update(data=typedef_dict)
        print(f'Assignment of "{super_type}" Entity to existing "{ends_with}" entities Completed.\n')

    def create_or_update(self, typedef_dict, info, attempt=1):
        try:
            print(f"Trying to create {info} Entity")
            self.driver.typedefs.create(data=typedef_dict)

        except Conflict:
            print(f"Already Exists, updating {info} Entity")
            try:
                self.driver.typedefs.update(data=typedef_dict)
            except Exception as ex:
                raise HttpError(message="Something wrong happened: {0}".format(str(ex)))

        except Timeout as ex:
            # Sometimes on local atlas instance you do get ReadTimeout a lot.
            # This will try to apply definition 3 times and then cancel
            if attempt < 4:
                print("ReadTimeout - Another Try: {0}".format(str(ex)))
                self.create_or_update(typedef_dict, info, attempt + 1)
            else:
                print("ReadTimeout Exception - Cancelling Operation: {0}".format(str(ex)))
        except Exception as ex:
            raise HttpError(message="Something wrong happened: {0}".format(str(ex)))
        finally:
            print(f"Applied {info} Entity Definition")
            print(f"\n----------")

    @property
    def driver(self):
        return AtlasClient().driver()

    def get_schema_dict(self, schema):
        return json.loads(schema)

    def create_table_schema(self):
        self.create_or_update(self.get_schema_dict(table_schema), "Table")

    def create_column_schema(self):
        self.create_or_update(self.get_schema_dict(column_schema), "Column")

    def create_user_schema(self):
        self.create_or_update(self.get_schema_dict(user_schema), "User")

    def create_reader_schema(self):
        self.create_or_update(self.get_schema_dict(reader_schema), "Reader")

    def create_user_reader_relation(self):
        self.create_or_update(self.get_schema_dict(user_reader_relation), "User <-> Reader")

    def create_metadata_schema(self):
        self.create_or_update(self.get_schema_dict(metadata_schema), "Metadata")

    def create_reader_metadata_relation(self):
        self.create_or_update(self.get_schema_dict(reader_metadata_relation), "Reader <-> Metadata")

    def create_table_metadata_schema(self):
        self.create_or_update(self.get_schema_dict(table_metadata_schema), "Table Metadata")

    def create_column_metadata_schema(self):
        self.create_or_update(self.get_schema_dict(column_metadata_schema), "Column Metadata")

    # noinspection PyMethodMayBeStatic
    def create_metadata(self, table_entity):
        """
        database.table.metadata@cluster
        """
        table_qn = table_entity.attributes.get("qualifiedName")
        table_info = parse_table_qualified_name(table_qn)
        table_guid = table_entity.guid

        metadata_qn = f'{table_info["db_name"]}.{table_info["table_name"]}.metadata@{table_info["cluster_name"]}'
        metadata_guid = table_guid * 999

        metadata_entity = {'typeName': 'table_metadata',
                           'guid': metadata_guid,
                           'attributes': {'qualifiedName': metadata_qn,
                                          'popularityScore': 0,
                                          'table': {'guid': table_guid}}
                           }

        return metadata_entity

    def initiate_existing_data(self):
        limit = 50
        offset = 0
        results = True
        count_query = {'query': "from Table where  __state = 'ACTIVE' select count()"}
        count_results = list(self.driver.search_dsl(**count_query))[0]
        count_value = count_results._data['attributes']['values'][0][0]

        while results:
            params = {'typeName': 'Table',
                      'attributes': ['metadata'],
                      'limit': limit,
                      'offset': offset
                      }
            search_results = self.driver.search_basic.create(data=params)

            entities_to_create = list()
            for entity in search_results.entities:
                if not entity.attributes.get("metadata"):
                    entities_to_create.append(self.create_metadata(entity))

            if entities_to_create:
                self.driver.entity_bulk.create(data={"entities": entities_to_create})

            if count_value > 0 and offset <= count_value:
                offset += 50
            else:
                results = False

    def create_required_entities(self, fix_existing_data=False):
        """
        IMPORTANT: The order of the entity definition matters.
        Please keep this order.
        :return: Creates or Updates the entity definition in Apache Atlas
        """
        self.create_table_schema()
        self.assign_subtypes(ends_with="_table", super_type="Table")
        self.create_column_schema()
        self.assign_subtypes(ends_with="_column", super_type="Column")
        self.create_user_schema()
        self.create_reader_schema()
        self.create_user_reader_relation()
        self.create_metadata_schema()
        self.create_reader_metadata_relation()
        self.create_table_metadata_schema()
        self.create_column_metadata_schema()

        if fix_existing_data:
            self.initiate_existing_data()
