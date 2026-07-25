import logging
import os
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
import pymysql
import pymysql.cursors
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_format: str | None = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    log_format = log_format or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


@dataclass
class DatabaseCredentials:
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = "postgresql"

    @classmethod
    def from_env_file(cls, env_path: str | Path, prefix: str, driver: str) -> "DatabaseCredentials":
        """
        Load credentials from a .env file using variables matching a prefix pattern.

        Expected variables:
            prefix_HOST, prefix_PORT, prefix_DATABASE, prefix_USERNAME,
            prefix_PASSWORD, prefix_DRIVER (optional)
        """
        env_vars = cls._parse_env_file(env_path)

        def get_var(name: str, default: str | None = None) -> str:
            key = f"{prefix}{name}"
            value = env_vars.get(key) or os.environ.get(key) or default
            if value is None:
                raise ValueError(f"Missing required environment variable: {key}")
            return value

        return cls(
            host=get_var("HOST"),
            port=int(get_var("PORT", "5432")),
            database=get_var("DATABASE"),
            username=get_var("USER"),
            password=get_var("PASSWORD"),
            driver=get_var("DRIVER", driver),
        )

    @staticmethod
    def _parse_env_file(env_path: str | Path) -> dict[str, str]:
        env_vars = {}
        path = Path(env_path)

        if not path.exists():
            return env_vars

        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
                if match:
                    key, value = match.groups()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or (
                        value.startswith("'") and value.endswith("'")
                    ):
                        value = value[1:-1]
                    env_vars[key] = value

        return env_vars

    @property
    def connection_string(self) -> str:
        encoded_password = quote_plus(self.password)
        return (
            f"{self.driver}://{self.username}:{encoded_password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def redacted_connection_string(self) -> str:
        return f"{self.driver}://{self.username}:****@****:{self.port}/{self.database}"

    def __str__(self) -> str:
        return (
            f"DatabaseCredentials(driver={self.driver!r}, "
            f"host='****', port={self.port}, database={self.database!r}, "
            f"username={self.username!r}, password='****')"
        )

    def __repr__(self) -> str:
        return self.__str__()


def pg_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        reraise=True,
    )


def mysql_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((pymysql.OperationalError, pymysql.InterfaceError)),
        reraise=True,
    )


class MySQLEngine:
    def __init__(
        self,
        creds: DatabaseCredentials,
        db_name: str | None = None,
    ) -> None:
        self.creds = creds
        self.db_name = db_name or creds.database
        self._conn: pymysql.Connection | None = None
        self.logger = get_logger("mysql_engine")

    def _connect(self) -> pymysql.Connection:
        print(
            f"Connecting with: host={self.creds.host}, port={self.creds.port}, user={self.creds.username}"
        )
        return pymysql.connect(
            host=self.creds.host,
            port=int(self.creds.port),
            user=self.creds.username,
            password=self.creds.password,
            database=self.db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )

    @property
    def conn(self) -> pymysql.Connection:
        if self._conn is None or not self._conn.open:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and self._conn.open:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @mysql_retry()
    def query(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()

    @mysql_retry()
    def execute(self, sql: str, params: tuple | None = None) -> int:
        with self.cursor() as cur:
            result = cur.execute(sql, params)
            self.conn.commit()
            return result

    @mysql_retry()
    def upsert_batch(
        self,
        table: str,
        records: list[dict],
        update_columns: list[str] | None = None,
    ) -> int:
        if not records:
            return 0

        columns = list(records[0].keys())
        update_cols = update_columns or columns

        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(f"`{c}`" for c in columns)
        update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)

        sql = f"""
            insert into `{table}` ({columns_str})
            values ({placeholders})
            on duplicate key update {update_clause}
        """

        values = [tuple(r[col] for col in columns) for r in records]

        with self.cursor() as cur:
            result = cur.executemany(sql, values)
            self.conn.commit()
            return result

    def upsert_batches(
        self,
        table: str,
        batches: Iterator[list[dict]],
        update_columns: list[str] | None = None,
    ) -> int:
        total = 0
        for batch in batches:
            total += self.upsert_batch(table, batch, update_columns)
        return total

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
