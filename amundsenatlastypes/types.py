import pkg_resources


def get_schema(schema):
    return pkg_resources.resource_string(__name__, schema).decode('utf-8')


table_schema = get_schema("schema/01_table_schema.json")
user_schema = get_schema("schema/02_user.json")
reader_schema = get_schema("schema/03_reader.json")
user_reader_relation = get_schema("schema/04_user_reader_relation.json")
metadata_schema = get_schema("schema/05_metadata.json")
metadata_dataset_relation = get_schema("schema/06_metadata_dataset_relation.json")
