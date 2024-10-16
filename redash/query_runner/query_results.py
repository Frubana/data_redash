import logging
import time
import re
import sqlite3

from redash import models
from redash.models import Group, ParameterizedQuery
from redash.models.parameterized_query import InvalidParameterError, QueryDetachedFromDataSourceError
from redash.permissions import has_access, view_only, has_permission
from redash.query_runner import (
    BaseQueryRunner,
    TYPE_STRING,
    guess_type,
    register,
    JobTimeoutException,
)
from redash.utils import json_dumps, json_loads, gen_query_hash
from redash.tasks import Job
from redash.serializers import serialize_job
from redash.tasks.queries import enqueue_query

logger = logging.getLogger(__name__)


class PermissionError(Exception):
    pass


class CreateTableError(Exception):
    pass


def extract_query_ids(query):
    queries = re.findall(r"(?:join|from)\s+query_(\d+)", query, re.IGNORECASE)
    return [int(q) for q in queries]


def extract_cached_query_ids(query):
    queries = re.findall(r"(?:join|from)\s+cached_query_(\d+)", query, re.IGNORECASE)
    return [int(q) for q in queries]


def _load_query(user, query_id):
    query = models.Query.get_by_id(query_id)

    if user.org_id != query.org_id:
        raise PermissionError("Query id {} not found.".format(query.id))

    # TODO: this duplicates some of the logic we already have in the redash.handlers.query_results.
    # We should merge it so it's consistent.
    if not has_access(query.data_source, user, view_only, Group.EXECUTE_QUERY_PERMISSION):
        raise PermissionError("You do not have access to query id {}.".format(query.id))

    return query


def _annotate_query(query_runner, query, user):
    metadata = {}
    metadata["Query Hash"] = query.query_hash
    metadata["Username"] = user.email
    metadata["query_id"] = query.id
    metadata["Scheduled"] = query.schedule is not None
    # metadata["Job ID"] =  job.id # esta informacion se encuentra en execution.py#246
    # se deberia pasar toda la metadata del executor cuando se llaman al run_query(), pero hay que modificar todos los query_runners
    return query_runner.annotate_query(query.query_text, metadata)


def get_query_results(user, query_id, bring_from_cache, parameters={}, is_scheduled=False):
    query = _load_query(user, query_id)

    if query.is_archived and not has_permission('admin', user):
        raise Exception("The query {} is archived and cannot be executed.".format(query.id))

    if bring_from_cache:
        if query.latest_query_data_id is not None:
            results = query.latest_query_data.data
        else:
            raise Exception("No cached result available for query {}.".format(query.id))
    else:
        execute_query = True

        if query.max_cache_time:
            query_result = models.QueryResult.get_latest(query.data_source, query.query_text, query.max_cache_time)

            if query_result:
                logger.info(f'Found Query Result Cache for intermediate query id=%s.', query.id)
                results = query_result.data
                execute_query = False

        if execute_query:
            error = None
            has_error = False
            query_result_id = 0

            query_hash = gen_query_hash(query.query_text)

            parameterized_query = ParameterizedQuery(query.query_text, org=query.org)

            try:
                parameterized_query.apply(parameters)
                if parameterized_query.missing_params:
                    _parameters = {p["name"]: p.get("value") for p in query.parameters}
                    if any(_parameters):
                        parameterized_query.apply(_parameters)
            except InvalidParameterError as e:
                msg = "Failed loading parameters for query query_hash={} id={} because of invalid parameters.".format(query_hash, query.id)
                logger.error(msg, e)
                raise Exception(msg)
            except QueryDetachedFromDataSourceError as e:
                msg = ("Failed loading parameters for query query_hash={} id={} because a related dropdown query ({}) "
                       "is unattached to any datasource.").format(query_hash, query.id, e.query_id)
                logger.error(msg, e)
                raise Exception(msg)

            query_text = query.data_source.query_runner.apply_auto_limit(
                parameterized_query.text, False
            )

            if parameterized_query.missing_params:
                msg = "Missing parameter value for: {} for query query_hash={} id={}.".format(", ".join(parameterized_query.missing_params),
                                                                                                      query_hash, query.id)
                logger.error(msg)
                raise Exception(msg)

            # only 1 concurrent execution of a query
            job = enqueue_query(
                query_text,
                query.data_source,
                user.id,
                user.is_api_user(),
                query if is_scheduled else None,
                metadata={
                    "Username": repr(user) if user.is_api_user() else user.email,
                    "query_id": query_id,
                    "parameters": parameters
                },
            )

            job_id = job.id

            while True:
                job_dict = serialize_job(Job.fetch(job_id))

                if job_dict['job']['status'] in [1, 2]:
                    time.sleep(1)
                    logger.info(f'The intermediate query query_hash=%s id=%s is still executing.', query_hash, query.id)
                    continue
                if job_dict['job']['status'] == 3:
                    query_result_id = job_dict['job']['query_result_id']
                    break
                if job_dict['job']['status'] == 4:
                    error = job_dict['job']['error']
                    has_error = True
                    break
                logger.warning(f'Unknown status for the job of the intermediate query query_hash=%s id=%s.', query_hash, query.id)
                error = 'Unknown Job Status'
                has_error = True
                break

            if has_error:
                if error:
                    raise Exception(
                        "Failed loading results for query query_hash={} id={}. Error: {}".format(query_hash, query.id,
                                                                                                 error))
                else:
                    raise Exception(
                        "Failed loading results for query query_hash={} id={}. Unknown Error: Worker Killed by SIGKILL"
                        .format(query_hash, query.id,))
            else:
                logger.info(f'Retrieving intermediate result of query query_hash=%s id=%s in Query Result'
                            , query_hash, query.id)

                query_result = models.QueryResult.get_by_id(query_result_id)

                if user.org_id != query_result.org_id:
                    raise PermissionError("The intermediate query id {} not belongs to the organization of the user.".format(query.id))

                results = query_result.data

    return results


def create_tables_from_query_ids(user, connection, query_ids, cached_query_ids=[], parameters={}, is_scheduled=False):
    for query_id in set(cached_query_ids):
        results = get_query_results(user, query_id, True, parameters, is_scheduled)
        table_name = "cached_query_{query_id}".format(query_id=query_id)
        create_table(connection, table_name, results)

    for query_id in set(query_ids):
        results = get_query_results(user, query_id, False, parameters, is_scheduled)
        table_name = "query_{query_id}".format(query_id=query_id)
        create_table(connection, table_name, results)


def fix_column_name(name):
    return '"{}"'.format(re.sub('[:."\s]', "_", name, flags=re.UNICODE))


def flatten(value):
    if isinstance(value, (list, dict)):
        return json_dumps(value)
    else:
        return value


def create_table(connection, table_name, query_results):
    try:
        columns = [column["name"] for column in query_results["columns"]]
        safe_columns = [fix_column_name(column) for column in columns]

        column_list = ", ".join(safe_columns)
        create_table = "CREATE TABLE {table_name} ({column_list})".format(
            table_name=table_name, column_list=column_list
        )
        logger.debug("CREATE TABLE query: %s", create_table)
        connection.execute(create_table)
    except sqlite3.OperationalError as exc:
        raise CreateTableError(
            "Error creating table {}: {}".format(table_name, str(exc))
        )

    insert_template = "insert into {table_name} ({column_list}) values ({place_holders})".format(
        table_name=table_name,
        column_list=column_list,
        place_holders=",".join(["?"] * len(columns)),
    )

    for row in query_results["rows"]:
        values = [flatten(row.get(column)) for column in columns]
        connection.execute(insert_template, values)


class Results(BaseQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {"type": "object", "properties": {}}

    @classmethod
    def name(cls):
        return "Query Results"

    def run_query(self, query, user, metadata):
        connection = sqlite3.connect(":memory:")

        safe_params = {}
        if 'parameters' in metadata:
            safe_params = metadata.get("parameters")

        query_ids = extract_query_ids(query)
        cached_query_ids = extract_cached_query_ids(query)
        create_tables_from_query_ids(user
                                     ,connection, query_ids
                                     , cached_query_ids
                                     , safe_params
                                     , metadata['Scheduled'] if 'Scheduled' in metadata else False)

        cursor = connection.cursor()

        try:
            cursor.execute(query)

            if cursor.description is not None:
                columns = self.fetch_columns([(i[0], None) for i in cursor.description])

                rows = []
                column_names = [c["name"] for c in columns]

                for i, row in enumerate(cursor):
                    for j, col in enumerate(row):
                        guess = guess_type(col)

                        if columns[j]["type"] is None:
                            columns[j]["type"] = guess
                        elif columns[j]["type"] != guess:
                            columns[j]["type"] = TYPE_STRING

                    rows.append(dict(zip(column_names, row)))

                data = {"columns": columns, "rows": rows}
                error = None
                json_data = json_dumps(data)
            else:
                error = "Query completed but it returned no data."
                json_data = None
        except (KeyboardInterrupt, JobTimeoutException):
            connection.cancel()
            raise
        finally:
            connection.close()
        return json_data, error


register(Results)
