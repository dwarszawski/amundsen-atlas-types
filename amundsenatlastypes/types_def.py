import pkg_resources


def get_schema(schema):
    return pkg_resources.resource_string(__name__, schema).decode('utf-8')


table_schema = get_schema("schema/01_table_schema.json")
column_schema = get_schema("schema/01_1_column_schema.json")
user_schema = get_schema("schema/02_user.json")
reader_schema = get_schema("schema/03_reader.json")
user_reader_relation = get_schema("schema/04_user_reader_relation.json")
reader_metadata_relation = get_schema("schema/05_0_reader_metadata_relation.json")
metadata_schema = get_schema("schema/05_metadata.json")
table_partition_schema = get_schema("schema/06_table_partition_schema.json")
hive_table_partition = get_schema("schema/06_1_hive_table_partition.json")
column_partition_schema = get_schema("schema/07_column_partition_schema.json")
