import json
import os

# noinspection PyPackageRequirements
from atlasclient.client import Atlas
# noinspection PyPackageRequirements
from atlasclient.exceptions import Conflict

from .types import *


class AtlasClient:
    host = os.environ.get('ATLAS_HOST', 'localhost')
    port = os.environ.get('ATLAS_PORT', 21000)
    user = os.environ.get('ATLAS_USERNAME', 'admin')
    password = os.environ.get('ATLAS_PASSWORD', 'admin')

    def driver(self):
        return Atlas(host=self.host,
                     port=self.port,
                     username=self.user,
                     password=self.password)


# noinspection PyMethodMayBeStatic
class Initializer:

    def assign_table_subtypes(self):
        print("Assigning 'Table' entity to all the table entity definitions")
        entities_to_update = []
        for t in self.driver.typedefs:
            for e in t.entityDefs:
                if e.name.endswith('_table'):  # Assign new entity to all the tables in atlas
                    print(f'Assigning {e.name} as a subtype of "Table"')
                    super_types = e.superTypes  # Get a property first to inflate the relational objects
                    ent_dict = e._data
                    ent_dict["superTypes"] = super_types
                    ent_dict["superTypes"].append("Table")
                    entities_to_update.append(ent_dict)

        typedef_dict = {
            "entityDefs": entities_to_update
        }
        self.driver.typedefs.update(data=typedef_dict)
        print("Assignment of Table Entity to existing table Completed.")

    def create_or_update(self, typedef_dict, info):
        try:
            print(f"Trying to create {info} Entity")
            self.driver.typedefs.create(data=typedef_dict)
        except Conflict as ex:
            print("Exception: {0}".format(str(ex)))
            print(f"Already Exists, updating {info} Entity")
            self.driver.typedefs.update(data=typedef_dict)
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

    def create_user_schema(self):
        self.create_or_update(self.get_schema_dict(user_schema), "User")

    def create_reader_schema(self):
        self.create_or_update(self.get_schema_dict(reader_schema), "Reader")

    def create_user_reader_relation(self):
        self.create_or_update(self.get_schema_dict(user_reader_relation), "User <-> Reader")

    def create_metadata_schema(self):
        self.create_or_update(self.get_schema_dict(metadata_schema), "Metadata")

    def create_metadata_dataset_relation(self):
        self.create_or_update(self.get_schema_dict(metadata_dataset_relation), "Metadata <-> DataSet")

    def create_required_entities(self):
        """
        IMPORTANT: The order of the entity definition matters.
        Please keep this order.
        :return: Creates or Updates the entity definition in Apache Atlas
        """
        self.create_table_schema()
        self.assign_table_subtypes()
        self.create_user_schema()
        self.create_reader_schema()
        self.create_user_reader_relation()
        self.create_metadata_schema()
        self.create_metadata_dataset_relation()
