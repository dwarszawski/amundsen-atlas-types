import json

# noinspection PyPackageRequirements
from atlasclient.exceptions import Conflict
from requests import Timeout

from amundsenatlastypes.client import driver
from .kickstart import KickstartExistingData
from .types import *


# noinspection PyMethodMayBeStatic
class Initializer:
    def assign_subtypes(self, ends_with="_table", super_type="Table"):
        print(f'\nAssigning {super_type} entity to all the subtypes entity definitions')
        entities_to_update = []
        for t in driver.typedefs:
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
        driver.typedefs.update(data=typedef_dict)
        print(f'Assignment of "{super_type}" Entity to existing "{ends_with}" entities Completed.\n')

    def create_or_update(self, typedef_dict, info, attempt=1):
        try:
            print(f"Trying to create {info} Entity")
            driver.typedefs.create(data=typedef_dict)

        except Conflict:
            print(f"Already Exists, updating {info} Entity")
            try:
                driver.typedefs.update(data=typedef_dict)
            except Exception as ex:
                # This is a corner case, for Atlas Sample Data
                print(f"Something wrong happened: {str(ex)}")

        except Timeout as ex:
            # Sometimes on local atlas instance you do get ReadTimeout a lot.
            # This will try to apply definition 3 times and then cancel
            if attempt < 4:
                print("ReadTimeout - Another Try: {0}".format(str(ex)))
                self.create_or_update(typedef_dict, info, attempt + 1)
            else:
                print("ReadTimeout Exception - Cancelling Operation: {0}".format(str(ex)))
        except Exception as ex:
            print(f"Something wrong happened: {str(ex)}")
        finally:
            print(f"Applied {info} Entity Definition")
            print(f"\n----------")

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

    def create_partition_schema(self):
        self.create_or_update(self.get_schema_dict(partition_schema), "Partition Entity")
        self.create_or_update(self.get_schema_dict(hive_table_partition_schema),"Hive Table Partition")

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
        self.create_partition_schema()

        if fix_existing_data:

            # noinspection PyShadowingNames,SpellCheckingInspection
            kickstart = KickstartExistingData()
            kickstart.initiate_existing_data()
