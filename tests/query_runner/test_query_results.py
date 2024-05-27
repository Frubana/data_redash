import sqlite3
from unittest import TestCase

from rq import Connection
from rq.job import JobStatus

from redash import rq_redis_connection
import pytest
import mock
import datetime
from mock import patch, Mock
from redash.query_runner.query_results import (
    CreateTableError,
    PermissionError,
    _load_query,
    create_table,
    extract_cached_query_ids,
    extract_query_ids,
    get_query_results,
    fix_column_name,
)
from redash import models
from redash.utils import json_dumps
from tests import BaseTestCase
from redash.utils import utcnow
from redash.tasks import Job


def fetch_job(*args, **kwargs):
    if any(args):
        job_id = args[0] if isinstance(args[0], str) else args[0].id
    else:
        job_id = create_job().id

    result = Mock()
    result.id = job_id
    result.is_cancelled = False

    return result


def create_job(*args, **kwargs):
    return Job(connection=rq_redis_connection)


class TestExtractQueryIds(TestCase):
    def test_works_with_simple_query(self):
        query = "SELECT 1"
        self.assertEqual([], extract_query_ids(query))

    def test_finds_queries_to_load(self):
        query = "SELECT * FROM query_123"
        self.assertEqual([123], extract_query_ids(query))

    def test_finds_queries_in_joins(self):
        query = "SELECT * FROM query_123 JOIN query_4566"
        self.assertEqual([123, 4566], extract_query_ids(query))

    def test_finds_queries_with_whitespace_characters(self):
        query = "SELECT * FROM    query_123 a JOIN\tquery_4566 b ON a.id=b.parent_id JOIN\r\nquery_78 c ON b.id=c.parent_id"
        self.assertEqual([123, 4566, 78], extract_query_ids(query))


class TestCreateTable(TestCase):
    def test_creates_table_with_colons_in_column_name(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "ga:newUsers"}, {"name": "test2"}],
            "rows": [{"ga:newUsers": 123, "test2": 2}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_creates_table_with_double_quotes_in_column_name(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "ga:newUsers"}, {"name": '"test2"'}],
            "rows": [{"ga:newUsers": 123, '"test2"': 2}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_creates_table(self):
        connection = sqlite3.connect(":memory:")
        results = {"columns": [{"name": "test1"}, {"name": "test2"}], "rows": []}
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_creates_table_with_missing_columns(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "test1"}, {"name": "test2"}],
            "rows": [{"test1": 1, "test2": 2}, {"test1": 3}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_creates_table_with_spaces_in_column_name(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "two words"}, {"name": "test2"}],
            "rows": [{"two words": 1, "test2": 2}, {"test1": 3}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_creates_table_with_dashes_in_column_name(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "two-words"}, {"name": "test2"}],
            "rows": [{"two-words": 1, "test2": 2}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")
        connection.execute('SELECT "two-words" FROM query_123')

    def test_creates_table_with_non_ascii_in_column_name(self):
        connection = sqlite3.connect(":memory:")
        results = {
            "columns": [{"name": "\xe4"}, {"name": "test2"}],
            "rows": [{"\xe4": 1, "test2": 2}],
        }
        table_name = "query_123"
        create_table(connection, table_name, results)
        connection.execute("SELECT 1 FROM query_123")

    def test_shows_meaningful_error_on_failure_to_create_table(self):
        connection = sqlite3.connect(":memory:")
        results = {"columns": [], "rows": []}
        table_name = "query_123"
        with pytest.raises(CreateTableError):
            create_table(connection, table_name, results)

    def test_loads_results(self):
        connection = sqlite3.connect(":memory:")
        rows = [{"test1": 1, "test2": "test"}, {"test1": 2, "test2": "test2"}]
        results = {"columns": [{"name": "test1"}, {"name": "test2"}], "rows": rows}
        table_name = "query_123"
        create_table(connection, table_name, results)
        self.assertEqual(len(list(connection.execute("SELECT * FROM query_123"))), 2)

    def test_loads_list_and_dict_results(self):
        connection = sqlite3.connect(":memory:")
        rows = [{"test1": [1, 2, 3]}, {"test2": {"a": "b"}}]
        results = {"columns": [{"name": "test1"}, {"name": "test2"}], "rows": rows}
        table_name = "query_123"
        create_table(connection, table_name, results)
        self.assertEqual(len(list(connection.execute("SELECT * FROM query_123"))), 2)


class TestGetQuery(BaseTestCase):
    # test query from different account
    def test_raises_exception_for_query_from_different_account(self):
        query = self.factory.create_query()
        user = self.factory.create_user(org=self.factory.create_org())

        self.assertRaises(PermissionError, lambda: _load_query(user, query.id))

    def test_raises_exception_for_query_with_different_groups(self):
        ds = self.factory.create_data_source(group=self.factory.create_group())
        query = self.factory.create_query(data_source=ds)
        user = self.factory.create_user()

        self.assertRaises(PermissionError, lambda: _load_query(user, query.id))

    def test_returns_query(self):
        query = self.factory.create_query()
        user = self.factory.create_user()

        loaded = _load_query(user, query.id)
        self.assertEqual(query, loaded)

    def test_returns_query_when_user_has_view_only_access(self):
        ds = self.factory.create_data_source(
            group=self.factory.org.default_group, view_only=True
        )
        query = self.factory.create_query(data_source=ds)
        user = self.factory.create_user()

        loaded = _load_query(user, query.id)
        self.assertEqual(query, loaded)


class TestExtractCachedQueryIds(TestCase):
    def test_works_with_simple_query(self):
        query = "SELECT 1"
        self.assertEqual([], extract_cached_query_ids(query))

    def test_finds_queries_to_load(self):
        query = "SELECT * FROM cached_query_123"
        self.assertEqual([123], extract_cached_query_ids(query))

    def test_finds_queries_in_joins(self):
        query = "SELECT * FROM cached_query_123 JOIN cached_query_4566"
        self.assertEqual([123, 4566], extract_cached_query_ids(query))

    def test_finds_queries_with_whitespace_characters(self):
        query = "SELECT * FROM    cached_query_123 a JOIN\tcached_query_4566 b ON a.id=b.parent_id JOIN\r\ncached_query_78 c ON b.id=c.parent_id"
        self.assertEqual([123, 4566, 78], extract_cached_query_ids(query))


class TestFixColumnName(TestCase):
    def test_fix_column_name(self):
        self.assertEqual('"a_b_c_d"', fix_column_name("a:b.c d"))


@patch("redash.tasks.queries.execution.Job.fetch", side_effect=fetch_job)
@patch("redash.tasks.queries.execution.Queue.enqueue", side_effect=create_job)
class TestGetQueryResult(BaseTestCase):
    def test_cached_query_result(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        self.assertEqual(query_result.data, get_query_results(self.factory.user, query.id, True))

    def test_non_cached_query_result(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        with Connection(rq_redis_connection):
            def pending_job(*args, **kwargs):  # simulamos que el job se esta ejecutando
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.STARTED
                return job

            def finish_job(*args, **kwargs):  # simulamos que el job termino
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.FINISHED
                job.query_result_id = 123
                return job

            with patch.object(models.QueryResult, "get_by_id_and_org") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}

                fetch.side_effect = [pending_job(), finish_job()]

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                qr.return_value = query_result_mock

                self.assertEqual(query_result_data, get_query_results(self.factory.user, query.id, False))
                self.assertEqual(1, enqueue.call_count)

    def test_non_cached_query_result_should_raise_error(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        with Connection(rq_redis_connection):
            def pending_job(*args, **kwargs):  # simulamos que el job se esta ejecutando
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.STARTED
                return job

            def failed_job(*args, **kwargs):  # simulamos que el job termino
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.FAILED
                job.error = 'an error'
                return job

            fetch.side_effect = [pending_job(), failed_job()]

            self.assertRaises(Exception, get_query_results, self.factory.user, query.id, False)
            self.assertEqual(2, fetch.call_count)
            self.assertEqual(1, enqueue.call_count)

    def test_non_cached_with_max_cache_time_and_query_result(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result, max_cache_time=60)

        self.assertEqual(query_result.data, get_query_results(self.factory.user, query.id, False))

    def test_non_cached_with_max_cache_time_and_non_query_result(self, enqueue, fetch):
        yesterday = utcnow() - datetime.timedelta(days=1)
        query_result = self.factory.create_query_result(retrieved_at=yesterday)
        query = self.factory.create_query(latest_query_data=query_result, max_cache_time=60)

        with Connection(rq_redis_connection):
            def finish_job(*args, **kwargs):  # simulamos que el job que se ejeucta termino
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.FINISHED
                job.query_result_id = 123
                return job

            with patch.object(models.QueryResult, "get_by_id_and_org") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}
                fetch.side_effect = finish_job

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                qr.return_value = query_result_mock

                self.assertEqual(query_result_data, get_query_results(self.factory.user, query.id, False))
                self.assertEqual(1, enqueue.call_count)

    def test_non_cached_query_result_enqueue_of_same_query(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        with Connection(rq_redis_connection):
            def pending_job(*args, **kwargs):  # simulamos que el job se esta ejecutando
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.STARTED
                return job

            def finish_job(*args, **kwargs):  # simulamos que el job termino
                job = fetch_job(*args, **kwargs)
                job.get_status.return_value = JobStatus.FINISHED
                job.query_result_id = 123
                return job

            with patch.object(models.QueryResult, "get_by_id_and_org") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}

                fetch.side_effect = [pending_job(),  # se crea el job y luego entra al while a verificar estado
                                     finish_job(),  # while que esta verificando el estado
                                     pending_job(),  # validacion del enqueue_query
                                     pending_job(),  # while que esta verificando el estado
                                     finish_job(),  # while que esta verificando el estado
                                     pending_job(),  # validacion del enqueue_query
                                     pending_job(),  # while que esta verificando el estado
                                     finish_job()]  # while que esta verificando el estado
                # primero le decimos que se esta ejecutando y luego que termina
                # para cada ejecucion se simula lo mismo para que termine el test (se simula el paralelismo)
                # pero nunca lo sacamos al job de redis, por ende deberia existir uno solo job

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                qr.return_value = query_result_mock

                get_query_results(self.factory.user, query.id, False)
                get_query_results(self.factory.user, query.id, False)
                get_query_results(self.factory.user, query.id, False)

                self.assertEqual(1, enqueue.call_count)
