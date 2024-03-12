import logging
import time
import re
import sqlite3

from redash import models
from redash.permissions import has_access, view_only, has_permission
from redash.query_runner import (
    BaseQueryRunner,
    TYPE_STRING,
    guess_type,
    register,
    JobTimeoutException,
)
from redash.tasks.alerts import check_alerts_for_query
from redash.utils import json_dumps, json_loads, gen_query_hash, utcnow

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
    if not has_access(query.data_source, user, view_only):
        raise PermissionError("You do not have access to query id {}.".format(query.id))

    return query


def _annotate_query(query_runner, query, user):
    metadata = {}
    metadata["Query Hash"] = query.query_hash
    metadata["Username"] = user.email
    metadata["query_id"] = query.id
    metadata["Scheduled"] = query.schedule is not None
    #metadata["Job ID"] =  job.id # esta informacion se encuentra en execution.py#246
    #se deberia pasar toda la metadata del executor cuando se llaman al run_query(), pero hay que modificar todos los query_runners
    return query_runner.annotate_query(query.query_text, metadata)


def get_query_results(user, query_id, bring_from_cache):
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
        started_at = time.time()

        if query.max_cache_time:
            query_result = models.QueryResult.get_latest(query.data_source, query.query_text, query.max_cache_time)

            if query_result:
                logger.info(f'Found Query Result Cache for intermediate query id=%s.', query.id)
                results = query_result.data
                execute_query = False

        if execute_query:
            query_runner = query.data_source.query_runner
            annotated_query = _annotate_query(query_runner, query, user)
            results, error = query_runner.run_query(
                annotated_query, user
            )
            run_time = time.time() - started_at

            if error:
                raise Exception("Failed loading results for query id {}. Error: {}".format(query.id, error))
            else:
                query_hash = gen_query_hash(query.query_text)
                logger.info(f'Saving intermediate result of query (%s) id=%s in Query Result', query_hash, query.id)
                query_result = models.QueryResult.store_result(
                    query.data_source.org_id,
                    query.data_source,
                    query_hash,
                    query.query_text,
                    results,
                    run_time,
                    utcnow()
                )
                updated_query_ids = models.Query.update_latest_result(query_result)
                models.db.session.commit()
                logger.info("checking_alerts")
                for query_id in updated_query_ids:
                    check_alerts_for_query.delay(query_id)
                results = json_loads(results)
                logger.info("finished")

    return results


def create_tables_from_query_ids(user, connection, query_ids, cached_query_ids=[]):
    for query_id in set(cached_query_ids):
        results = get_query_results(user, query_id, True)
        table_name = "cached_query_{query_id}".format(query_id=query_id)
        create_table(connection, table_name, results)

    for query_id in set(query_ids):
        results = get_query_results(user, query_id, False)
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

    def run_query(self, query, user):
        connection = sqlite3.connect(":memory:")

        query_ids = extract_query_ids(query)
        cached_query_ids = extract_cached_query_ids(query)
        create_tables_from_query_ids(user, connection, query_ids, cached_query_ids)

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
