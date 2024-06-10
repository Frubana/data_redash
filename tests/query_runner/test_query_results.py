import sqlite3
from unittest import TestCase

from rq import Connection
from rq.exceptions import NoSuchJobError
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
            pending_job = fetch_job()
            pending_job.get_status.return_value = JobStatus.STARTED

            finish_job = fetch_job()
            finish_job.get_status.return_value = JobStatus.FINISHED
            finish_job.result = 123

            with patch.object(models.QueryResult, "get_by_id") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}

                fetch.side_effect = [pending_job, finish_job]

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                query_result_mock.org_id = self.factory.org.id
                qr.return_value = query_result_mock

                result_data = get_query_results(self.factory.user, query.id, False)

                self.assertEqual(1, enqueue.call_count)
                self.assertEqual(2, fetch.call_count)
                self.assertEqual(query_result_data, result_data)

    def test_non_cached_query_result_should_raise_error(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        with Connection(rq_redis_connection):
            pending_job = fetch_job()
            pending_job.get_status.return_value = JobStatus.STARTED

            failed_job = fetch_job()
            failed_job.get_status.return_value = JobStatus.FAILED
            failed_job.result = 'an error'

            fetch.side_effect = [pending_job, failed_job]

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
            finish_job = fetch_job()
            finish_job.get_status.return_value = JobStatus.FINISHED
            finish_job.result = 123

            with patch.object(models.QueryResult, "get_by_id") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}
                fetch.side_effect = [finish_job]

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                query_result_mock.org_id = self.factory.org.id
                qr.return_value = query_result_mock

                self.assertEqual(query_result_data, get_query_results(self.factory.user, query.id, False))
                self.assertEqual(1, enqueue.call_count)
                self.assertEqual(1, fetch.call_count)

    def test_non_cached_query_result_enqueue_of_same_query(self, enqueue, fetch):
        query_result = self.factory.create_query_result()
        query = self.factory.create_query(latest_query_data=query_result)

        with Connection(rq_redis_connection):
            pending_job = fetch_job()
            pending_job.get_status.return_value = JobStatus.STARTED

            finish_job = fetch_job()
            finish_job.get_status.return_value = JobStatus.FINISHED
            finish_job.result = 123

            with patch.object(models.QueryResult, "get_by_id") as qr:
                query_result_data = {"columns": [{"name": "id", "friendly_name": "id", "type": "integer"}],
                                     "rows": [{"id": 99}]}

                fetch.side_effect = [pending_job,  # se crea el job y luego entra al while a verificar estado
                                     finish_job,  # while que esta verificando el estado
                                     pending_job,  # validacion del enqueue_query
                                     pending_job,  # while que esta verificando el estado
                                     finish_job,  # while que esta verificando el estado
                                     pending_job,  # validacion del enqueue_query
                                     pending_job,  # while que esta verificando el estado
                                     finish_job]  # while que esta verificando el estado
                # primero le decimos que se esta ejecutando y luego que termina
                # para cada ejecucion se simula lo mismo para que termine el test (se simula el paralelismo)
                # pero nunca lo sacamos al job de redis, por ende deberia existir uno solo job

                query_result_mock = Mock()
                query_result_mock.data = query_result_data
                query_result_mock.org_id = self.factory.org.id
                qr.return_value = query_result_mock

                get_query_results(self.factory.user, query.id, False)
                get_query_results(self.factory.user, query.id, False)
                get_query_results(self.factory.user, query.id, False)

                self.assertEqual(1, enqueue.call_count)
                self.assertEqual(8, fetch.call_count)
