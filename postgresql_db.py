"""
PostgreSQL 表查询服务：
- 连接信息从 `app.config.settings` 读取（环境变量 / .env）。
- 表名使用配置项或调用方传入的伪代码占位名，部署时替换为真实表名。
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote_plus

import asyncpg

from app.config import settings

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_READ_SQL_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def _pg_dsn() -> str:
    if settings.pg_url:
        return settings.pg_url
    user = quote_plus(settings.pg_user)
    password = quote_plus(settings.pg_password)
    return (
        f"postgresql://{user}:{password}@{settings.pg_host}:{settings.pg_port}/{settings.pg_database}"
    )


def _qualified_table(table: str, schema: str | None = None) -> str:
    """校验标识符并返回 schema.table 形式（用于 SQL 拼接）。"""
    sch = schema or settings.pg_schema
    for part in (sch, table):
        if not _IDENT_RE.match(part):
            raise ValueError(f"非法表名或 schema 标识符: {part!r}")
    return f'"{sch}"."{table}"'


class PostgreSQLStore:
    """封装连接池与常用表查询。"""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=_pg_dsn(),
                min_size=settings.pg_pool_min_size,
                max_size=settings.pg_pool_max_size,
            )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def fetch_rows(
        self,
        table: str,
        *,
        columns: list[str] | None = None,
        where_clause: str | None = None,
        params: tuple[Any, ...] = (),
        order_by: str | None = None,
        limit: int = 100,
        offset: int = 0,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        按表查询多行，返回 dict 列表。

        - `where_clause` 仅写条件表达式，使用 $1、$2 占位，例如 ``status = $1``。
        - `order_by` 为已校验的列名或 ``col DESC`` 形式（不含用户原始输入时可直接传入伪代码列名）。
        """
        if columns:
            for col in columns:
                if not _IDENT_RE.match(col):
                    raise ValueError(f"非法列名: {col!r}")
            col_sql = ", ".join(f'"{c}"' for c in columns)
        else:
            col_sql = "*"

        qtable = _qualified_table(table, schema)
        sql = f"SELECT {col_sql} FROM {qtable}"
        args: list[Any] = list(params)

        if where_clause:
            sql += f" WHERE {where_clause}"

        if order_by:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\s+(ASC|DESC))?$", order_by, re.IGNORECASE):
                raise ValueError(f"非法 order_by: {order_by!r}")
            sql += f" ORDER BY {order_by}"

        sql += f" LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}"
        args.extend([limit, offset])

        pool = await self.pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

    async def fetch_one(
        self,
        table: str,
        *,
        where_clause: str,
        params: tuple[Any, ...],
        columns: list[str] | None = None,
        schema: str | None = None,
    ) -> dict[str, Any] | None:
        rows = await self.fetch_rows(
            table,
            columns=columns,
            where_clause=where_clause,
            params=params,
            limit=1,
            schema=schema,
        )
        return rows[0] if rows else None

    async def fetch_by_id(
        self,
        table: str,
        row_id: Any,
        *,
        id_column: str = "id",
        columns: list[str] | None = None,
        schema: str | None = None,
    ) -> dict[str, Any] | None:
        if not _IDENT_RE.match(id_column):
            raise ValueError(f"非法主键列名: {id_column!r}")
        return await self.fetch_one(
            table,
            where_clause=f'"{id_column}" = $1',
            params=(row_id,),
            columns=columns,
            schema=schema,
        )

    async def execute(
        self,
        sql: str,
        *args: Any,
    ) -> str:
        """执行写操作（INSERT/UPDATE/DELETE 等），返回 asyncpg 状态字符串。"""
        result = await self.custom_sql(sql, *args, fetch=False)
        assert isinstance(result, str)
        return result

    async def custom_sql(
        self,
        sql: str,
        *params: Any,
        fetch: bool | None = None,
    ) -> list[dict[str, Any]] | str:
        """
        自定义 SQL：自行编写完整语句，占位符用 $1、$2，参数通过 *params 传入（勿拼接用户输入）。

        - 查询（SELECT / WITH）：返回 ``list[dict]``
        - 写操作：返回 asyncpg 状态字符串，如 ``UPDATE 3``
        - ``fetch=None`` 时按 SQL 首关键字自动判断；也可显式传 ``fetch=True/False``
        """
        is_query = fetch if fetch is not None else bool(_READ_SQL_RE.match(sql))
        pool = await self.pool()
        async with pool.acquire() as conn:
            if is_query:
                rows = await conn.fetch(sql, *params)
                return [dict(r) for r in rows]
            return await conn.execute(sql, *params)

    async def run_sql(
        self,
        sql: str,
        *params: Any,
        fetch: bool | None = None,
    ) -> list[dict[str, Any]] | str:
        """``run_sql`` 为 ``custom_sql`` 的别名。"""
        return await self.custom_sql(sql, *params, fetch=fetch)

    # ---------- 伪代码业务表示例（表名来自 settings） ----------

    async def list_example_records(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """伪代码：查询 example_records 表。"""
        return await self.fetch_rows(
            settings.pg_table_example,
            order_by="id",
            limit=limit,
            offset=offset,
        )

    async def get_metadata_by_doc_id(self, doc_id: str) -> dict[str, Any] | None:
        """伪代码：按 doc_id 查询 doc_metadata 表。"""
        return await self.fetch_one(
            settings.pg_table_metadata,
            where_clause="doc_id = $1",
            params=(doc_id,),
        )


_pg_singleton: PostgreSQLStore | None = None


def get_pg() -> PostgreSQLStore:
    global _pg_singleton
    if _pg_singleton is None:
        _pg_singleton = PostgreSQLStore()
    return _pg_singleton
