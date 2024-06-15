import json
import logging
from threading import Lock, Semaphore

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from tenacity import (
    after_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)


class TimescaleDBClient:
    """Usage:
    .write(tb_name, data, tags, auto_mode=True):
        Params:
        - tb_name: the database table to write data
        - data: must be a dict of lists as defined by class TimescaledbInputData
        - tags: a list specifying which columns will be included in unique constraint.
        - auto_mode: client will auto create not exisited table, add columns, update
            unique constraint, remap invalid columns names if nessesary.

    .write_df(tb_name, df, tags)):
        a wrapper on .write() that take pandas df as input data

    .query(sql)
        Params:
        - sql: the sql string to execute
        return: a tuple (columns name, list(rows value) )

    .query_df():
        a wrapper on .query()
        return: pandas dataframe


    Example:
        # create connection
        tsdb = TimescaleDBClient(
                    host="111.111.111.11", # or "localhost"
                    port=5432,
                    user="postgres",
                    db_name = 'mydatabase',
                    password="xxxxx",
        )
        tsdb.query('SELECT datname FROM pg_database;')
        tsdb.query('select * from mytable')
        tsdb.query(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'public'"
        )
    """

    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        after=after_log(logger, logging.WARNING),
        stop=stop_after_attempt(3),
    )
    def __init__(
        self,
        host="localhost",
        port=5432,
        user="postgres",
        password=None,
        db_name="postgres",
        max_conn=10,
        min_conn=0,
        pooling_mode=True,
        **kwargs,
    ):
        # blocking thread and wait when exceed max connections
        self.semaphore = Semaphore(max_conn)
        self.db_name = db_name

        self.conn_profile = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db_name,
        )
        # why keep connection open and reuse cursor:
        # https://www.psycopg.org/docs/faq.html#best-practices
        # why bother using connection pool?
        # https://bbengfort.github.io/2017/12/psycopg2-transactions/

        self.pooling_mode = pooling_mode
        if self.pooling_mode:

            def _init_pool():
                self.conn_pool = ThreadedConnectionPool(
                    min_conn,
                    max_conn,
                    **self.conn_profile,
                    **kwargs,
                )

            def recycle_conn(db_conn):
                self.conn_pool.putconn(db_conn)

            self._init_pool = _init_pool
            self.recycle_conn = recycle_conn
            self._init_pool()  # first call to create pool
        else:

            def recycle_conn(db_conn):
                db_conn.close()

            self.recycle_conn = recycle_conn

        self.alter_table_lock = Lock()

    def _notify(self, cur, tb_name, notify_tp):
        min_tp, max_tp = notify_tp
        msg = f"{tb_name}|{min_tp}|{max_tp}"
        cur.execute(f"NOTIFY {self.db_name}, '{msg}';")

    # will reinit pool when fail, in case of DB down
    @retry(wait=wait_fixed(10))
    def try_get_conn(self):
        if self.pooling_mode:
            try:
                db_conn = self.conn_pool.getconn()
            except Exception as e:
                logger.error("Fail to get db connection, restarting conn pool...")
                self._init_pool()
                raise e
        else:
            try:
                db_conn = psycopg2.connect(**self.conn_profile)
            except Exception as e:
                logger.error("Fail to connect db, retry after 10s")
                raise e

        return db_conn

    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        after=after_log(logger, logging.WARNING),
        stop=stop_after_attempt(10),
    )
    def _execute_query(self, sql):
        self.semaphore.acquire()
        db_conn = self.try_get_conn()

        try:
            with db_conn:
                with db_conn.cursor() as cursor:
                    cursor.execute(sql)
                    cur_des = cursor.description
                    cur_re = cursor.fetchall() if cur_des else None
        except psycopg2.OperationalError as e:
            logger.warning(f"{e}: {sql} retrying...")
            raise e
        except psycopg2.errors.UndefinedTable as e:
            logger.warning(f"{e}: cursor returned as NONE")
            cur_des, cur_re = None, None
        finally:
            self.recycle_conn(db_conn)
            self.semaphore.release()

        return cur_des, cur_re

    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        after=after_log(logger, logging.WARNING),
        stop=stop_after_attempt(10),
    )
    def _execute_values(self, sql, records, notify_tp=None, tb_name=None):
        self.semaphore.acquire()
        db_conn = self.try_get_conn()

        try:
            with db_conn:
                # auto close cursor when leaving the second 'with'
                with db_conn.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, sql, records, page_size=10000)
                    if notify_tp is not None:
                        self._notify(cursor, tb_name, notify_tp)
        finally:
            self.recycle_conn(db_conn)
            self.semaphore.release()

    def _get_columns(self, table_name) -> list:
        cur_des, cur_re = self._execute_query(
            f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS " f"WHERE TABLE_NAME = '{table_name}';"
        )
        re_sql = [t[0] for t in cur_re]
        return re_sql

    def _get_uniq_columns(self, tb_name) -> list:
        query = """
                SELECT
                    att.attname AS conname
                FROM
                    pg_index idx
                JOIN pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = ANY(idx.indkey)
                WHERE
                    idx.indexrelid = '{index_name}'::regclass;
                """
        sql = query.format(index_name=tb_name + "_uidx")
        cur_des, cur_re = self._execute_query(sql)
        return [t[0] for t in cur_re] if cur_re else []

    def _get_indexed_columns(self, table_name) -> set:
        cur_des, cur_re = self._execute_query(
            """
            select
                a.attname as column_name
            from
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a
            where
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and a.attnum = ANY(ix.indkey)
                and t.relkind = 'r'
            """
            f"and t.relname = '{table_name}';"
        )
        re_sql = list({t[0] for t in cur_re})
        return re_sql

    def _update_unique_index(self, tb_name, tags):
        # sorted() to put 'interval' before 'symbol' for indexing,delete it and becareful of the order
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"Given tags {tags} must start with [interval, c_symbol...]"
        u_str = json.dumps(tags)[1:-1] + " ,time DESC"

        self._execute_query(
            f'DROP INDEX IF EXISTS "{tb_name}_uidx";' f'CREATE UNIQUE INDEX "{tb_name}_uidx" on {tb_name} ({u_str});'
        )
        logger.warning(f"table {tb_name} unique index updated to {u_str}")

    def currentDB_tables(self) -> list:
        cur_des, cur_re = self._execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        re_sql = [t[0] for t in cur_re]
        return re_sql

    def create_millis_now(self, tb_name, segment=None):
        cur_des, cur_re = self._execute_query(  # set the timestamp unit for now()
            f"CREATE OR REPLACE FUNCTION millis_now () "
            f"returns BIGINT LANGUAGE SQL STABLE as $$ "
            f"SELECT (extract( epoch from now() ) * 1000 )::BIGINT $$; "
            f"SELECT set_integer_now_func('{tb_name}', 'millis_now', replace_if_exists => TRUE); "
        )
        logger.warning(f"Create millis_now() for {tb_name} ")

    def update_columns(
        self,
        tb_name: str,
        cols: list,
        tags: list,
        col_nonnumeric=None,
        allow_compression=True,
    ):
        if col_nonnumeric is None:
            col_nonnumeric = dict()

        existed_tags = self._get_uniq_columns(tb_name)
        if "time" in existed_tags:  # Never treat time as tag
            existed_tags.remove("time")
        existed_cols = self._get_columns(tb_name)

        assert set(tags).issubset(cols), "tags must be a subset of columns!"
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"Given tags {tags} must start with [interval, c_symbol...]"

        cols_to_add = set(cols) - set(existed_cols)
        for col in cols_to_add:
            sql = f"ALTER TABLE {tb_name} "
            if col in tags:
                # add column as tag with indexing
                sql += f'ADD COLUMN "{col}" TEXT NOT NULL;'
            elif col in ("ingestion_tp", "last_update_tp"):
                # ingestion_tp/last_update_tp is timestamp when writing in millisecond, so column should be bigint
                sql += f'ADD COLUMN "{col}" BIGINT;'
            elif col_nonnumeric.get(col):
                # store as string if failed to convert to numeric
                sql += f'ADD COLUMN "{col}" TEXT;'
            else:
                # add colume as field without indexing
                sql += f'ADD COLUMN "{col}" DOUBLE PRECISION;'

            self._execute_query(sql)
            logger.critical(f"Column {col} is added to table {tb_name}")

        if set(existed_tags) != set(tags):
            logger.warning(
                f"{tb_name}'s tag columns changed from {existed_tags} to {tags} "
                f"unique index key need to be rebuilt!"
            )
            self._update_unique_index(tb_name, tags)
        else:
            logger.warning(f"{tb_name}'s tag columns remain unchanged: {existed_tags}" f"skip unique index be rebuilt!")

    def create_hypertable(self, tb_name, data=None, chunk_millis=86400000 * 7):
        cur_des, cur_re = self._execute_query(
            f'CREATE TABLE IF NOT EXISTS "{tb_name}" (time BIGINT NOT NULL);'
            f"SELECT create_hypertable('{tb_name}', 'time', if_not_exists => TRUE, "
            f"chunk_time_interval => {chunk_millis});"
        )
        logger.warning(f"Create table {tb_name} with chunk size in millis: {chunk_millis}")

    def _persist(self, tb_name, col_names, records, notify_tp=None):
        cols_str = json.dumps(col_names)[1:-1]
        sql = f"INSERT INTO {tb_name} " f"({cols_str}) " f"VALUES %s"
        self._execute_values(sql, records, notify_tp, tb_name)
        logger.debug(f"Persist {len(records)} records into {tb_name}")

    def _write(self, tb_name, data, auto_mode=False, notify_tp=None):
        tags = data["tags"]
        assert set(tags).issubset(data["columns"]), "tags must be a subset of columns!"
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"Given tags {tags} must start with [interval, c_symbol...]"

        if auto_mode:
            with self.alter_table_lock:
                if tb_name not in self.currentDB_tables():
                    # create table if not existed
                    self.create_hypertable(tb_name, data)
                    # infer column type by trying to convert first row
                    df = pd.DataFrame(data["records"])
                    first_row = df.fillna(method="bfill", axis=0).iloc[0]
                    c_r = pd.to_numeric(first_row.to_numpy(), errors="coerce")
                    col_nonnumeric = dict(zip(data["columns"], np.isnan(c_r)))
                    self.update_columns(
                        tb_name=tb_name,
                        tags=data["tags"],
                        cols=data["columns"],
                        col_nonnumeric=col_nonnumeric,
                    )
                    # auto correct invalid column names

        cols_str = json.dumps(data["columns"])[1:-1]
        tags_str = json.dumps(list({*data["tags"], "time"}))[1:-1]
        # why excluded.t?
        # https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT
        set_str = ",".join([f'"{t}" = excluded."{t}"' for t in data["columns"] if t != "ingestion_tp"])
        sql = (
            f"INSERT INTO {tb_name} "
            f"({cols_str}) "
            f"VALUES %s ON CONFLICT  "
            f"({tags_str}) "
            f"DO UPDATE SET "
            f"{set_str}"
        )
        try:
            self._execute_values(sql, data["records"], notify_tp, tb_name)
        except (
            psycopg2.errors.UndefinedColumn,  # columns changed
            psycopg2.errors.InvalidColumnReference,  # or tags changed
            psycopg2.errors.InvalidTextRepresentation,
        ) as e:
            if not auto_mode:
                raise e
            logging.warning(
                f"{str(e)}: fail writing at first attemp, "
                f"update columns {data['columns']} with tag {data['tags']} and retry"
            )
            df = pd.DataFrame(data["records"])
            first_row = df.fillna(method="bfill", axis=0).iloc[0]
            c_r = pd.to_numeric(first_row.to_numpy(), errors="coerce")
            col_nonnumeric = dict(zip(data["columns"], np.isnan(c_r)))
            # try to guess value type
            self.update_columns(
                tb_name,
                tags=data["tags"],
                cols=data["columns"],
                col_nonnumeric=col_nonnumeric,
            )
            # try again
            self._execute_values(sql, data["records"], notify_tp, tb_name)
            logging.warning("Success at second attempt to write with updated columns!")

    def write_df(self, tb_name, df, tags, auto_mode=False, validation=True):
        notify_tp = df["time"].min(), df["time"].max()
        if validation:
            df = df.drop_duplicates()
            timestamp_len = len(str(df["time"].iloc[0]))
            assert timestamp_len == 13, "timestamp not in millis sec!"
        data = {"columns": list(df.columns), "tags": tags, "records": df.to_numpy()}

        self._write(tb_name, data, auto_mode, notify_tp)

    def query(self, sql):
        try:
            cur_des, cur_re = self._execute_query(sql)
        except psycopg2.errors.UndefinedTable as e:
            logging.warning(str(e))
            cur_des = None
        if cur_des is not None:
            columns = [desc[0] for desc in cur_des]
            return columns, cur_re
        else:
            return None, None

    def query_df(self, sql):
        columns, records = self.query(sql)
        if columns:
            return pd.DataFrame(records, columns=columns)
        else:
            return pd.DataFrame()
