import logging
import re
from random import randint, random, sample
from typing import Dict, List

from atlasclient.utils import make_table_qualified_name
from faker import Faker

from amundsenatlastypes import Initializer
from amundsenatlastypes.client import driver
from amundsenatlastypes.sample_data import SampleData

__all__ = ['SampleTableData']


class SampleTableData(SampleData):
    def __init__(self, **kwargs):
        self.driver = driver
        self.chet = Faker()

        self.db_count = kwargs.get('db_count', 4)
        self.table_count = kwargs.get('table_count', 5)
        self.partition_count = kwargs.get('partition_count', 3)
        self.user_count = kwargs.get('user_count', 10)

        self.cluster = kwargs.get('cluster', 'demo')

        self.created_databases: Dict[str, any] = dict()
        self.users_list: List[str] = list()
        self.created_users_entities = dict()

        self.initializer = Initializer()

    @staticmethod
    def _chunker(seq, size):
        return (seq[pos: pos + size] for pos in range(0, len(seq), size))

    def _load_words(self, words_count):
        words = list()

        for _ in range(words_count):
            word = re.sub('[^a-zA-Z0-9]', '_', self.chet.country()).lower()
            word = re.sub(r'\_+', '_', word)

            words.append(word)

        return words

    def _load_partitions(self, partitions):
        return [
            self.chet.date_this_year().strftime('%Y%m%d') for _ in range(partitions)
        ]

    def _load_users(self, users_count):
        users = ['testuser']

        for _ in range(users_count):
            user = re.sub(
                '[^a-zA-Z0-9]',
                '_',
                f'{self.chet.first_name()}_{self.chet.last_name()}'.lower(),
            )
            user = re.sub(r'\_+', '_', user)

            users.append(user)

        self.users_list = users
        return users

    def _create_users(self, users):
        user_entities = list()
        for user in users:

            user_entities.append(
                {'typeName': 'User', 'attributes': {'qualifiedName': user}}
            )

        if user_entities:
            self.driver.entity_bulk.create(data={'entities': user_entities})

    def _create_database(self, database_name):
        qn = f'{database_name}@{self.cluster}'

        if database_name not in self.created_databases:
            logging.info(f'Creating database {database_name}.')

            entity_dict = {
                'entity': {
                    'typeName': 'hive_db',
                    'attributes': {
                        'name': database_name,
                        'owner': 'admin',
                        'qualifiedName': qn,
                        'clusterName': self.cluster,
                    },
                }
            }

            self.driver.entity_post.create(data=entity_dict)
            db_entity = self.driver.entity_unique_attribute('hive_db', qualifiedName=qn)

            self.created_databases.update({database_name: db_entity})

        return self.created_databases[database_name]

    def _render_columns(self, table_name, db_name, table_guid):
        def render_statistics():
            result = []

            stats = [
                ('approximateNumDistinctValues', lambda: randint(1000, 3000)),
                ('sum', lambda: randint(900, 1000) + random()),
                ('max', lambda: randint(6, 15) + random()),
                ('min', lambda: randint(0, 5) + random()),
                ('completness', lambda: round(100 * random(), 2)),
                ('standard deviation', lambda: randint(1, 4) + random()),
                ('mean', lambda: round((randint(5, 7) + random()) / 10, 2)),
            ]

            for stat in stats:
                name, creator = stat

                result.append(
                    dict(stat_name=name, stat_val=creator(), start_epoch=0, end_epoch=0)
                )

            return result

        column_entities = list()

        for index in range(len(table_name)):
            column_name = f'{table_name}_column_{index}'
            column_qn = f'{db_name}.{table_name}.{column_name}@{self.cluster}'
            column_entities.append(
                {
                    'typeName': 'hive_column',
                    'attributes': {
                        'qualifiedName': column_qn,
                        'type': 'string',
                        'name': column_name,
                        'description': self.chet.paragraph(
                            nb_sentences=self.chet.random_int(min=2, max=4)
                        ),
                        'statistics': render_statistics(),
                        'table': {'guid': table_guid},
                    },
                }
            )

        return column_entities

    def _create_readers(self, readers, reader_qn_prefix, table_guid):
        '''
        database.table.user_qualifiedName.reader@cluster
        '''
        reader_entities = list()

        for index, reader in enumerate(readers):
            reader_count = len(readers) * index
            reader_qn = f'{reader_qn_prefix}.{reader}.reader@{self.cluster}'
            user_entity = self.created_users_entities.get(reader)

            if not user_entity:
                user_entity = self.driver.entity_unique_attribute(
                    'User', qualifiedName=reader
                )

            db, table = reader_qn_prefix.split('.')
            self.created_users_entities.update({reader: user_entity})
            reader_entities.append(
                {
                    'typeName': 'Reader',
                    'uniqueAttributes': {'qualifiedName': reader_qn},
                    'attributes': {
                        'qualifiedName': reader_qn,
                        'count': reader_count,
                        'entityUri': f'hive_table://{self.cluster}.{db}/{table}',
                        'user': {'guid': user_entity.entity['guid']},
                        'entity': {'guid': table_guid},
                    },
                }
            )

        return reader_entities

    def _render_reports(
            self, table_guid, qn_prefix, report_url='s3://metadata/reports/test.html'
    ):
        qn = f'{qn_prefix}.ap.test.html@{self.cluster}'

        report_ap = {
            'typeName': 'Report',
            'uniqueAttributes': {'qualifiedName': qn},
            'attributes': {
                'qualifiedName': qn,
                'name': 'Advanced Profile',
                'url': report_url,
                'entity': {'guid': str(table_guid)},
            },
        }

        qn = f'{qn_prefix}.dq.test.html'

        report_dq = {
            'typeName': 'Report',
            'uniqueAttributes': {'qualifiedName': qn},
            'attributes': {
                'qualifiedName': qn,
                'name': 'Data Quality',
                'url': report_url,
                'entity': {'guid': str(table_guid)},
            },
        }

        return [report_ap, report_dq]

    def _render_hive_partitions(
            self, database_name, table_name, table_guid, partitions
    ):
        bulk_entities = list()
        for index, partition in enumerate(partitions):
            bulk_entities.append(
                {
                    'typeName': 'hive_table_partition',
                    'attributes': {
                        'qualifiedName': f'{database_name}.{table_name}.{partition}@{self.cluster}',
                        'name': f'{partition}',
                        'table': {'guid': table_guid},
                    },
                }
            )

        return bulk_entities

    def _create_hive_tables(self, databases, partitions, table_count):
        logging.info(f'Creating tables for {len(databases)} databases.')
        bulk_entities = list()

        index = 0
        result = []
        for db in databases:
            database_entity = self._create_database(db).entity
            database_guid = database_entity['guid']
            database_name = database_entity['attributes']['name']

            for j in range(table_count):
                index += 1

                table_name = re.sub('[^a-zA-Z0-9]', '_', self.chet.company().lower())
                table_name = re.sub(r'\_+', '_', table_name)

                table_qn = make_table_qualified_name(table_name, self.cluster, db)

                logging.info(f'Creating table {table_qn}.')

                table_guid = (index + 1) * -1

                random_readers = self.chet.random.randint(
                    int(round(self.user_count / 4, 0)), self.user_count
                )

                reader_entities = list()

                if random_readers > 0:
                    readers = sample(self.users_list, random_readers)
                    reader_qn_prefix = f'{database_name}.{table_name}'

                    reader_entities = self._create_readers(
                        readers, reader_qn_prefix, table_guid
                    )

                table_description = self.chet.paragraph(
                    nb_sentences=self.chet.random_int(min=3, max=5)
                )

                bulk_entities.append(
                    {
                        'typeName': 'hive_table',
                        'guid': table_guid,
                        'attributes': {
                            'qualifiedName': table_qn,
                            'name': table_name,
                            'description': table_description,
                            'db': {'guid': database_guid},
                            'owner': 'admin',
                            'parameters': {'loadType': 'INCREMENTAL'},
                            'popularityScore': self.chet.random.random(),
                        },
                    }
                )
                bulk_entities.extend(
                    self._render_columns(table_name, database_name, table_guid)
                )
                bulk_entities.extend(
                    self._render_hive_partitions(
                        table_name, database_name, table_guid, partitions
                    )
                )
                bulk_entities.extend(
                    self._render_reports(table_guid, f'{database_name}.{table_name}')
                )
                bulk_entities.extend(reader_entities)
                result.append((table_qn, table_guid))

        if bulk_entities:
            self.driver.entity_bulk.create(data={'entities': bulk_entities})

        return result

    def _create_lineage(self):
        def _get_tables():
            query_params = dict(typeName='Table', sortBy='popularityScore',
                                sortOrder='DESCENDING', excludeDeletedEntities=True, limit=10)

            results = self.driver.search_basic.create(data=query_params)

            return [(x.guid, x.attributes['qualifiedName'].split('@')[0]) for x in results.entities]

        pairs = [(1, 2), (2, 3), (4, 3), (3, 5), (3, 6), (6, 7), (5, 7)]

        tables = _get_tables()
        processes = []

        for i, pair in enumerate(pairs):
            job = f'test_job_{i}'

            ip, op = pair

            input_guid, input_name = tables[ip - 1]
            output_guid, output_name = tables[op - 1]

            inputs = list()
            inputs.append(dict(guid=input_guid))

            outputs = list()
            outputs.append(dict(guid=output_guid))

            job_process = dict(typeName='spark_process',
                               attributes=dict(user=job,
                                               userName='admin',
                                               operationType='CREATE',
                                               qualifiedName=f'spark_process.{job}@{self.cluster}',
                                               name=job,
                                               queryText='SELECT INTO {output_name} AS SELECT * FROM {input_name}'),
                               relationshipAttributes=dict(inputs=inputs, outputs=outputs))

            processes.append(job_process)

            self.driver.entity_bulk.create(data=dict(entities=processes))

            logging.info(f'Created lineage for tables:\n' + '\n'.join([x[1] for x in tables]))

    def _initialize(self):
        self.initializer.create_column_schema()
        self.initializer.create_reader_schema()
        self.initializer.create_user_schema()
        self.initializer.create_bookmark_schema()
        self.initializer.create_report_schema()
        self.initializer.create_table_schema()
        self.initializer.assign_subtypes(regex='(.*)_table$', super_type='Table')
        self.initializer.assign_subtypes(regex='(.*)_column$', super_type='Column')
        self.initializer.create_user_reader_relation()
        self.initializer.create_reader_referenceable_relation()
        self.initializer.create_table_partition_schema()
        self.initializer.create_hive_table_partition()
        self.initializer.create_data_owner_relation()

    def _create(self, *args, **kwargs):
        logging.info('Loading Sample Users')
        users_list = self._load_users(self.user_count)

        logging.info('Creating Users Entities')
        for users in self._chunker(users_list, 5):
            self._create_users(users)

        logging.info('Loading Sample Words')
        databases = self._load_words(self.db_count)
        partitions = self._load_partitions(self.partition_count)

        logging.info('Creating Hive Tables')
        for databases in self._chunker(databases, 2):
            self._create_hive_tables(databases, partitions, self.table_count)

        logging.info('Creating Lineage')
        self._create_lineage()
