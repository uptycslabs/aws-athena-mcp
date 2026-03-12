"""
AWS Athena client wrapper.

Simple, clean wrapper around boto3 Athena client with async support.
"""

import asyncio
import json
import logging
import re
import time
from typing import List, Optional, Union

import boto3
from botocore.exceptions import ClientError

from .config import Config
from .models import (
    DatabaseInfo, QueryRequest, QueryResult,
    QueryState, QueryStatus, TableInfo,
)

# Set up logging
logger = logging.getLogger(__name__)


class AthenaError(Exception):
    """Simple Athena error with code."""

    def __init__(
        self, message: str, code: str = "ATHENA_ERROR",
        query_execution_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.query_execution_id = query_execution_id


class QueryValidator:
    """Validates and sanitizes SQL queries."""

    # Note: Athena is read-only and IAM-controlled, so we only block
    # truly dangerous patterns (multi-statement injection, command execution).
    # Legitimate patterns like information_schema, UNION SELECT, and SQL
    # comments are allowed as they are valid Athena usage.
    DANGEROUS_PATTERNS = [
        r";\s*(drop|delete|truncate|alter|create|insert|update)\s+",
        r"xp_cmdshell",
        r"sp_executesql",
    ]

    @classmethod
    def validate_query(cls, query: str) -> None:
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        query_lower = query.lower()
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, query_lower, re.IGNORECASE | re.DOTALL):
                logger.warning(
                    f"Potentially dangerous SQL pattern: {pattern}"
                )
                raise ValueError(
                    f"Query contains potentially dangerous pattern: "
                    f"{pattern}"
                )

        if len(query) > 100000:
            raise ValueError("Query is too large (max 100KB)")

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        if not identifier or not identifier.strip():
            raise ValueError("Identifier cannot be empty")

        sanitized = re.sub(
            r"[^a-zA-Z0-9_-]", "", identifier.strip()
        )

        if not sanitized:
            raise ValueError(
                "Identifier contains only invalid characters"
            )
        if len(sanitized) > 255:
            raise ValueError(
                "Identifier is too long (max 255 characters)"
            )

        return sanitized


class AthenaClient:
    """AWS Athena client wrapper with per-call credential support."""

    def __init__(
        self, config: Config,
        profile_name: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        self.config = config
        self._session = None
        self._client = None

        self.profile_name = profile_name
        self.region_name = region_name or config.aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token

        logger.info(
            f"Initialized Athena client for region: {self.region_name}"
        )

    def _get_session(self):
        """Get or create a boto3 session with configured credentials."""
        if self._session is None:
            if self.aws_access_key_id and self.aws_secret_access_key:
                # Use directly provided credentials (e.g., from AssumeRole)
                self._session = boto3.Session(
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    aws_session_token=self.aws_session_token,
                    region_name=self.region_name,
                )
            else:
                # Use profile/region or default credential chain
                self._session = boto3.Session(
                    profile_name=self.profile_name,
                    region_name=self.region_name,
                )
        return self._session

    @property
    def client(self):
        """Lazy-initialize the Athena client."""
        if self._client is None:
            self._client = self._get_session().client("athena")
        return self._client

    async def execute_query(
        self, request: QueryRequest,
    ) -> Union[QueryResult, str]:
        """Execute a query and return results or execution ID if timeout."""
        logger.info(f"Executing query in database: {request.database}")

        try:
            QueryValidator.validate_query(request.query)
            sanitized_db = QueryValidator.sanitize_identifier(
                request.database
            )

            start_params = {
                "QueryString": request.query,
                "QueryExecutionContext": {"Database": sanitized_db},
                "ResultConfiguration": {
                    "OutputLocation": self.config.s3_output_location,
                },
            }

            if self.config.athena_workgroup:
                start_params["WorkGroup"] = self.config.athena_workgroup

            response = await asyncio.to_thread(
                self.client.start_query_execution, **start_params
            )
            qid = response["QueryExecutionId"]
            logger.info(f"Started query execution: {qid}")

            if await self._wait_for_completion(qid):
                logger.info(f"Query completed: {qid}")
                return await self.get_query_results(qid, request.max_rows)
            else:
                logger.warning(f"Query timed out: {qid}")
                return qid

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), code)
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

    async def get_query_status(
        self, query_execution_id: str,
    ) -> QueryStatus:
        """Get the status of a query execution."""
        try:
            response = await asyncio.to_thread(
                self.client.get_query_execution,
                QueryExecutionId=query_execution_id,
            )
            execution = response.get("QueryExecution", {})
            status = execution.get("Status", {})
            statistics = execution.get("Statistics", {})

            return QueryStatus(
                query_execution_id=query_execution_id,
                state=QueryState(status.get("State", "UNKNOWN")),
                state_change_reason=status.get("StateChangeReason"),
                bytes_scanned=statistics.get(
                    "DataScannedInBytes", 0
                ),
                execution_time_ms=statistics.get(
                    "EngineExecutionTimeInMillis", 0
                ),
            )

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), code, query_execution_id)

    async def get_query_results(
        self, query_execution_id: str, max_rows: int = 1000,
    ) -> QueryResult:
        """Get results for a completed query."""
        try:
            status = await self.get_query_status(query_execution_id)

            if status.state in [QueryState.RUNNING, QueryState.QUEUED]:
                raise AthenaError(
                    "Query is still running",
                    "QUERY_RUNNING", query_execution_id,
                )
            if status.state == QueryState.FAILED:
                reason = status.state_change_reason or "Query failed"
                raise AthenaError(
                    reason, "QUERY_FAILED", query_execution_id,
                )
            if status.state != QueryState.SUCCEEDED:
                raise AthenaError(
                    f"Query in unexpected state: {status.state}",
                    "UNEXPECTED_STATE", query_execution_id,
                )

            # Paginate results
            columns = []
            rows = []
            next_token = None
            is_first_page = True

            while len(rows) < max_rows:
                page_size = min(
                    max_rows - len(rows),
                    999 if is_first_page else 1000,
                )
                get_params = {
                    "QueryExecutionId": query_execution_id,
                    "MaxResults": page_size + (
                        1 if is_first_page else 0
                    ),
                }
                if next_token:
                    get_params["NextToken"] = next_token

                response = await asyncio.to_thread(
                    self.client.get_query_results, **get_params
                )
                result_set = response.get("ResultSet", {})

                if is_first_page:
                    column_info = result_set.get(
                        "ResultSetMetadata", {}
                    ).get("ColumnInfo", [])
                    columns = [
                        col.get("Name", "") for col in column_info
                    ]

                rows_data = result_set.get("Rows", [])
                start_idx = (
                    1 if is_first_page and rows_data and columns
                    else 0
                )

                for row_data in rows_data[start_idx:]:
                    if len(rows) >= max_rows:
                        break
                    row = {}
                    data_list = row_data.get("Data", [])
                    for i, data in enumerate(data_list):
                        if i < len(columns):
                            row[columns[i]] = data.get(
                                "VarCharValue"
                            )
                    rows.append(row)

                is_first_page = False
                next_token = response.get("NextToken")
                if not next_token:
                    break

            return QueryResult(
                query_execution_id=query_execution_id,
                columns=columns,
                rows=rows,
                row_count=len(rows),
                bytes_scanned=status.bytes_scanned,
                execution_time_ms=status.execution_time_ms,
            )

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), code, query_execution_id)

    async def list_databases(self) -> List[str]:
        """List all databases in the Athena catalog."""
        try:
            def _list_databases():
                databases = []
                paginator = self.client.get_paginator(
                    "list_databases"
                )
                for page in paginator.paginate(
                    CatalogName="AwsDataCatalog"
                ):
                    for db in page.get("DatabaseList", []):
                        databases.append(db.get("Name", ""))
                return sorted(databases)

            return await asyncio.to_thread(_list_databases)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), code)

    async def list_tables(self, database: str) -> DatabaseInfo:
        """List all tables in a database."""
        sanitized_db = QueryValidator.sanitize_identifier(database)
        request = QueryRequest(
            database=sanitized_db, query="SHOW TABLES", max_rows=1000,
        )
        result = await self.execute_query(request)

        if isinstance(result, str):
            raise AthenaError(
                "SHOW TABLES query timed out", "TIMEOUT", result,
            )

        tables = [
            row.get("tab_name", "")
            for row in result.rows if "tab_name" in row
        ]
        return DatabaseInfo(
            database=sanitized_db,
            tables=tables, table_count=len(tables),
        )

    async def describe_table(
        self, database: str, table_name: str,
    ) -> TableInfo:
        """Get schema information for a specific table."""
        sanitized_db = QueryValidator.sanitize_identifier(database)
        sanitized_tbl = QueryValidator.sanitize_identifier(table_name)
        request = QueryRequest(
            database=sanitized_db,
            query=f"DESCRIBE {sanitized_tbl}", max_rows=1000,
        )
        result = await self.execute_query(request)

        if isinstance(result, str):
            raise AthenaError(
                f"DESCRIBE {table_name} query timed out",
                "TIMEOUT", result,
            )

        columns = [
            {
                "name": row.get("col_name", ""),
                "type": row.get("data_type", ""),
                "comment": row.get("comment", ""),
            }
            for row in result.rows
        ]
        return TableInfo(
            database=sanitized_db,
            table_name=sanitized_tbl, columns=columns,
        )

    # JSON-returning methods (called by with_aws_config decorator)

    async def run_query(
        self, database: str, query: str, max_rows: int = 1000,
    ) -> str:
        """Execute a query and return JSON results."""
        if not database.strip():
            raise ValueError("Database name cannot be empty")
        if not query.strip():
            raise ValueError("Query cannot be empty")
        if max_rows < 1 or max_rows > 10000:
            raise ValueError("max_rows must be between 1 and 10000")

        request = QueryRequest(
            database=database, query=query, max_rows=max_rows,
        )
        result = await self.execute_query(request)

        if isinstance(result, QueryResult):
            return json.dumps(result.dict(), indent=2)
        else:
            return json.dumps(
                {
                    "query_execution_id": result,
                    "status": "timeout",
                    "message": "Query timed out",
                },
                indent=2,
            )

    async def list_databases_json(self) -> str:
        """List databases and return JSON."""
        databases = await self.list_databases()
        return json.dumps(
            {
                "databases": databases,
                "database_count": len(databases),
            },
            indent=2,
        )

    async def list_tables_json(self, database: str) -> str:
        """List tables and return JSON."""
        if not database.strip():
            raise ValueError("Database name cannot be empty")
        info = await self.list_tables(database)
        return json.dumps(info.dict(), indent=2)

    async def describe_table_json(
        self, database: str, table_name: str,
    ) -> str:
        """Describe table and return JSON."""
        if not database.strip():
            raise ValueError("Database name cannot be empty")
        if not table_name.strip():
            raise ValueError("Table name cannot be empty")
        info = await self.describe_table(database, table_name)
        return json.dumps(info.dict(), indent=2)

    async def _wait_for_completion(
        self, query_execution_id: str,
    ) -> bool:
        """Wait for query completion with timeout."""
        timeout = self.config.timeout_seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = await asyncio.to_thread(
                    self.client.get_query_execution,
                    QueryExecutionId=query_execution_id,
                )
                state = (
                    response.get("QueryExecution", {})
                    .get("Status", {})
                    .get("State")
                )

                if state == QueryState.SUCCEEDED:
                    return True
                if state in [
                    QueryState.FAILED, QueryState.CANCELLED
                ]:
                    reason = (
                        response.get("QueryExecution", {})
                        .get("Status", {})
                        .get("StateChangeReason", "Query failed")
                    )
                    raise AthenaError(
                        reason, "QUERY_FAILED", query_execution_id,
                    )

                await asyncio.sleep(0.5)

            except ClientError as e:
                code = e.response.get(
                    "Error", {}
                ).get("Code", "UNKNOWN")
                raise AthenaError(
                    str(e), code, query_execution_id,
                )

        return False
