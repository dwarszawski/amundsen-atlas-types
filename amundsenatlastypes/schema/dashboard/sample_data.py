from amundsenatlastypes import Initializer
from amundsenatlastypes.client import driver
from amundsenatlastypes.sample_data import SampleData

__all__ = ['SampleDashboardData']


class SampleDashboardData(SampleData):
    """
    Class for creating dummy data for dashboard entities.
    """

    def __init__(self, table_guid, user_guid):
        self.table_guid = table_guid
        self.user_guid = user_guid

        assert len(self.table_guid) > 0, 'Please set table_guid so dashboard is connected to existing Table entity'
        assert len(self.user_guid) > 0, 'Please set user_guid so dashboard is connected to existing User'

        self.initializer = Initializer()

    def _initialize(self):
        self.initializer.create_dashboard_group_schema()
        self.initializer.create_dashboard_schema()
        self.initializer.create_dashboard_query_schema()
        self.initializer.create_dashboard_chart_schema()
        self.initializer.create_dashboard_execution_schema()

    def _create(self, *args, **kwargs):
        dashboard_group_guid = '-1'
        dashboard_group_dict = {
            'guid': dashboard_group_guid,
            'typeName': 'DashboardGroup',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod',
                'name': 'prod superset',
                'id': 'prod',
                'description': 'Apache Superset Dashboards',
                'url': 'https://apache.superset',
            }
        }

        dashboard_guid = '-2'
        dashboard_dict = {
            'guid': dashboard_guid,
            'typeName': 'Dashboard',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1',
                'name': 'Prod Usage',
                'url': 'https://prod.superset/dashboards/1',
                'description': 'Robs famous dashboard',
                'createdTimestamp': 1619517099,
                'lastModifiedTimestamp': 1619626531,
                'cluster': 'datalab',
                'product': 'superset',
                'tables': [{'guid': self.table_guid}],
                'group': {'guid': dashboard_group_guid},
                'ownedBy': [{'guid': self.user_guid}]
            }
        }

        dashboard_execution_dict = {
            'typeName': 'DashboardExecution',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1/execution/1',
                'state': 'succeeded',
                'timestamp': 1619517099,
                'dashboard': {'guid': dashboard_guid}
            }
        }

        dashboard_query_guid_1 = '-3'
        dashboard_query_1_dict = {
            'guid': dashboard_query_guid_1,
            'typeName': 'DashboardQuery',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1/query/1',
                'name': 'Total Count',
                'id': 'total_count',
                'url': 'https://prod.superset/dashboards/1/query/1',
                'queryText': 'SELECT COUNT(1) FROM db.table',
                'dashboard': {'guid': dashboard_guid}
            }
        }

        dashboard_query_guid_2 = '-4'
        dashboard_query_2_dict = {
            'guid': dashboard_query_guid_2,
            'typeName': 'DashboardQuery',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1/query/2',
                'name': 'User Count By Time',
                'id': 'user_count_by_time',
                'url': 'https://prod.superset/dashboards/1/query/2',
                'queryText': 'SELECT date, COUNT(1) FROM db.table GROUP BY 1',
                'dashboard': {'guid': dashboard_guid}
            }
        }

        dashboard_chart_1_dict = {
            'typeName': 'DashboardChart',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1/query/1/chart/1',
                'name': 'Total Count',
                'type': 'metric',
                'url': 'https://prod.superset/dashboards/1/query/1/chart/1',
                'query': {'guid': dashboard_query_guid_1}
            }
        }

        dashboard_chart_2_dict = {
            'typeName': 'DashboardChart',
            'attributes': {
                'qualifiedName': 'superset_dashboard://datalab.prod/1/query/2/chart/1',
                'name': 'Count Users by Time',
                'type': 'horizontal_bar',
                'url': 'https://prod.superset/dashboards/1/query/2/chart/1',
                'query': {'guid': dashboard_query_guid_2}
            }
        }

        entities = [dashboard_group_dict,
                    dashboard_dict, dashboard_execution_dict,
                    dashboard_query_1_dict, dashboard_query_2_dict,
                    dashboard_chart_1_dict, dashboard_chart_2_dict]

        driver.entity_bulk.create(data={'entities': entities})
