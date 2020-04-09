from amundsenatlastypes import Initializer

init = Initializer()
init.create_required_entities()


"""
DB:                         [db]@[cluster]
Table:                      [db].[table]@[cluster]
Column:                     [db].[table].[column]@[cluster]
        
TableMetadata:              [db].[table].metadata@[cluster]
ColumnMetadata:             [db].[table].[column].metadata@[cluster]
Reader:                     [db].[table].[CK].reader@[cluster]

Partition:                  SuperType
hive_table_partition:       [db].[table].partition.[partitionName]@[cluster]

"""
