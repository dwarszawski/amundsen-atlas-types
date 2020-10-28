import pkg_resources


def get_schema(schema):
    return pkg_resources.resource_string(__name__, schema).decode('utf-8')


table_schema = get_schema("schema/01_2_table_schema.json")
bookmark_schema = get_schema("schema/01_3_bookmark.json")
report_schema = get_schema("schema/01_4_report.json")
column_schema = get_schema("schema/01_column_schema.json")
user_schema = get_schema("schema/02_user.json")
reader_schema = get_schema("schema/01_1_reader.json")
user_reader_relation = get_schema("schema/04_user_reader_relation.json")
reader_referenceable_relation = get_schema("schema/04_reader_referenceable_relation.json")
table_partition_schema = get_schema("schema/05_table_partition_schema.json")
hive_table_partition = get_schema("schema/05_1_hive_table_partition.json")
data_owner_schema = get_schema("schema/06_user_table_owner_relation.json")
