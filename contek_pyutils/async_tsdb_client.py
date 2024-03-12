import asyncio
import json
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg
from psycopg_pool.pool_async import AsyncConnectionPool
from tenacity import retry
from tenacity.after import after_log
from tenacity.stop import stop_after_attempt

logger = logging.getLogger(__name__)


class AsyncTsdbClient:
    """
    The prototype of this class is contek_pyutils/timescaledb.py,
    """

    CHUNK_MILLIS = 1000 * 60 * 60 * 24 * 7  # 7 days

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        db_name: str = "postgres",
        min_size: int = 0,
        max_size: int = 64,
        max_idle: int = 60,
        connection_timeout: int = 10,  # timeout for establishing a connection
    ):
        self._pool = AsyncConnectionPool(
            f"host={host} port={port} user={user} password={password} dbname={db_name}",
            min_size=min_size,
            max_size=max_size,
            timeout=connection_timeout,
            max_idle=max_idle,
            kwargs=dict(autocommit=True),
        )
        self._lock = asyncio.Lock()

    @property
    def stats(self) -> Dict[str, int]:
        return self._pool.get_stats()

    @property
    async def current_tables(self) -> List[str]:
        query = "SELECT table_name FROM information_schema.tables " "WHERE table_schema = 'public'"
        _, res = await self._execute_query(query)
        return [row[0] for row in res]

    async def _execute_query(self, query: str) -> Tuple[Any, Any]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query.encode())
                cur_des = cursor.description
                if cur_des is None:
                    return None, None
                else:
                    return cur_des, await cursor.fetchall()

    async def _get_columns(self, table_name: str) -> List[str]:
        query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS " f"WHERE TABLE_NAME = '{table_name}';"
        _, res = await self._execute_query(query)
        return [row[0] for row in res]

    async def _get_uniq_columns(self, table_name: str) -> List[str]:
        query = "SELECT conname FROM pg_constraint " f"WHERE conrelid = '{table_name}'::regclass::oid;"
        _, res = await self._execute_query(query)
        return [row[0] for row in res]

    async def _update_unique_index(self, tb_name: str, tags: list):
        # sorted() to put 'interval' before 'symbol' for indexing,delete it and becareful of the order
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"Given tags {tags} must start with [interval, c_symbol...]"
        u_str = json.dumps(tags)[1:-1] + " ,time DESC"

        await self._execute_query(
            f'DROP INDEX IF EXISTS "{tb_name}_uidx";' f'CREATE UNIQUE INDEX "{tb_name}_uidx" on {tb_name} ({u_str});'
        )
        logger.warning(f"table {tb_name} unique index updated to {u_str}")

    async def _update_columns(
        self,
        table_name: str,
        cols: list,
        tags: list,
        col_nonnumeric: Optional[dict] = None,
    ):
        col_nonnumeric = dict() if col_nonnumeric is None else col_nonnumeric
        existed_tags = await self._get_uniq_columns(table_name)
        if "time" in existed_tags:  # Never treat time as tag
            existed_tags.remove("time")
        existed_cols = await self._get_columns(table_name)

        assert set(tags).issubset(cols), "tags must be a subset of columns!"
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"Given tags {tags} must start with [interval, c_symbol...]"

        cols_to_add = set(cols) - set(existed_cols)
        for col in cols_to_add:
            query = f"ALTER TABLE {table_name} "
            if col == "ingestion_tp":
                # ingestion_tp record the last writing time
                query += f'ADD COLUMN "{col}" BIGINT;'
            elif col in tags:
                # add column as tag with indexing
                query += f'ADD COLUMN "{col}" TEXT NOT NULL;'
            elif col_nonnumeric.get(col):
                # store as string if failed to convert to numeric
                query += f'ADD COLUMN "{col}" TEXT;'
            else:
                # add colume as field without indexing
                query += f'ADD COLUMN "{col}" DOUBLE PRECISION;'

            await self._execute_query(query)
            logger.critical(f"Column {col} is added to table {table_name}")

        if existed_tags != set(tags):
            logger.warning(
                f"{table_name}'s tag columns changed from {existed_tags} to {tags} "
                f"unique index key need to be rebuilt!"
            )
            await self._update_unique_index(table_name, tags)

    async def _create_hypertable(self, table_name: str, tags: List[str], columns: List[str], col_nonnumeric: dict):
        query = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (time BIGINT NOT NULL);
            SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE,
            chunk_time_interval => {self.CHUNK_MILLIS});
        """
        # Creating tables at the same time sometimes has strange results in old timescaledb.py
        async with self._lock:
            await self._execute_query(query)
            await self._update_columns(table_name, columns, tags, col_nonnumeric)

        logger.warning(f"created hypertable {table_name}")

    @staticmethod
    def _get_col_nonnumeric(columns: List[str], records: List[List[Any]]) -> dict:
        df = pd.DataFrame(records)
        first_row = df.fillna(method="bfill", axis=0).iloc[0]
        c_r = np.array(pd.to_numeric(first_row.to_numpy(), errors="coerce"))
        col_nonnumeric = dict(zip(columns, list(np.isnan(c_r))))
        return col_nonnumeric

    @retry(
        after=after_log(logger, logging.ERROR),
        stop=stop_after_attempt(3),
    )
    async def write(
        self,
        table_name: str,
        tags: List[str],
        columns: List[str],
        records: List[list],
    ):
        assert set(tags).issubset(set(columns)), "tags must be a subset of columns"
        assert (
            len(tags) >= 2 and tags[0] == "interval" and tags[1] == "c_symbol"
        ), f"tags {tags} must start with [interval, c_symbol...]"

        col_nonnumeric = self._get_col_nonnumeric(columns, records)

        cols_str = json.dumps(columns)[1:-1]
        tags_str = json.dumps(list({*tags, "time"}))[1:-1]
        set_str = ",".join([f'"{t}" = excluded."{t}"' for t in columns])
        query = f"""
            INSERT INTO {table_name} ({cols_str}) VALUES
            ({', '.join(['%s' for i in range(len(columns))])})
            ON CONFLICT ({tags_str}) DO UPDATE SET {set_str}
            """
        try:
            async with self._pool.connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(query.encode(), records)

        except BaseException as e:
            match type(e):
                case psycopg.errors.UndefinedTable:
                    logger.warning(f"table {table_name} not found, creating...")
                    await self._create_hypertable(table_name, tags, columns, col_nonnumeric)

                case psycopg.errors.UndefinedColumn:
                    logger.warning(f"{table_name}: updating columns...")
                    await self._update_columns(table_name, columns, tags, col_nonnumeric)

                case psycopg.errors.InvalidColumnReference:
                    logger.warning(f"{table_name}: updating index...")
                    await self._update_unique_index(table_name, tags)

            logger.error("".join(traceback.TracebackException.from_exception(e).format()))
            raise e

    async def query_df(self, query: str) -> pd.DataFrame:
        cur_des, cur_res = await self._execute_query(query)
        if cur_des is None:
            return pd.DataFrame()
        else:
            return pd.DataFrame(cur_res, columns=[c.name for c in cur_des])
