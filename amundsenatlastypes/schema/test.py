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

DashboardGroup:             [product]_dashboard://[cluster].[group_id]
Dashboard:                  [product]_dashboard://[cluster].[group_id]/[dashboard_id]
DashboardChart:             [product]_dashboard://[cluster].[group_id]/[dashboard_id]/chart/[chart_id]
DashboardQuery:             [product]_dashboard://[cluster].[group_id]/[dashboard_id]/chart/[chart_id]/query/[query_id]
DashboardExecution:         [product]_dashboard://[cluster].[group_id]/[dashboard_id]/execution/[execution_id]
"""
