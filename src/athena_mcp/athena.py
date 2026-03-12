"""
AWS Athena client wrapper.

Simple, clean wrapper around boto3 Athena client with async support.
"""

import asyncio
import logging
import re
import time
from typing import List, Optional, Union

import boto3
from botocore.exceptions import ClientError

from .config import Config
from .models import DatabaseInfo, QueryRequest, QueryResult, QueryState, QueryStatus, TableInfo

# Set up logging
logger = logging.getLogger(__name__)


class AthenaError(Exception):
    """Simple Athena error with code."""

    def __init__(
        self, message: str, code: str = "ATHENA_ERROR", query_execution_id: Optional[str] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.query_execution_id = query_execution_id


class QueryValidator:
    """Validates and sanitizes SQL queries to prevent injection attacks."""

    # Dangerous SQL patterns that should be blocked
    # Note: Athena is read-only and IAM-controlled, so we only block
    # truly dangerous patterns (multi-statement injection, command execution).
    # Legitimate patterns like information_schema, UNION SELECT, and SQL
    # comments are allowed as they are valid Athena usage.
    DANGEROUS_PATTERNS = [
        r";\s*(drop|delete|truncate|alter|create|insert|update)\s+",
        r"xp_cmdshell",  # Command execution
        r"sp_executesql",  # Dynamic SQL execution
    ]

    @classmethod
    def validate_query(cls, query: str) -> None:
        """
        Validate SQL query for potential injection attacks.

        Args:
            query: SQL query to validate

        Raises:
            ValueError: If query contains dangerous patterns
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        query_lower = query.lower()

        # Check for dangerous patterns
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, query_lower, re.IGNORECASE | re.DOTALL):
                logger.warning(f"Potentially dangerous SQL pattern detected: {pattern}")
                raise ValueError(f"Query contains potentially dangerous pattern: {pattern}")

        # Additional validation
        if len(query) > 100000:  # 100KB limit
            raise ValueError("Query is too large (max 100KB)")

        logger.debug(f"Query validation passed for query of length {len(query)}")

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        """
        Sanitize database/table identifiers.

        Args:
            identifier: Database or table name

        Returns:
            Sanitized identifier

        Raises:
            ValueError: If identifier is invalid
        """
        if not identifier or not identifier.strip():
            raise ValueError("Identifier cannot be empty")

        # Remove any non-alphanumeric characters except underscores and hyphens
        sanitized = re.sub(r"[^a-zA-Z0-9_-]", "", identifier.strip())

        if not sanitized:
            raise ValueError("Identifier contains only invalid characters")

        if len(sanitized) > 255:
            raise ValueError("Identifier is too long (max 255 characters)")

        logger.debug(f"Sanitized identifier: {identifier} -> {sanitized}")
        return sanitized


class AthenaClient:
    """Simple AWS Athena client wrapper."""

    def __init__(self, config: Config):
        self.config = config
        self._default_client = None

        # Only create a default client if we have default credentials
        if not config.no_default_creds:
            self._default_client = self._create_client(config)

        logger.info(
            f"Initialized Athena client for region: {config.aws_region}, "
            f"no_default_creds: {config.no_default_creds}"
        )

    @staticmethod
    def _create_client(config: Config):
        """Create a boto3 Athena client from config."""
        session_kwargs = {"region_name": config.aws_region}

        # Use per-call credentials if provided
        if config.aws_access_key_id and config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = config.aws_secret_access_key
            if config.aws_session_token:
                session_kwargs["aws_session_token"] = config.aws_session_token

        session = boto3.Session(**session_kwargs)
        return session.client("athena")

    def _get_client(self, config: Optional[Config] = None):
        """Get the appropriate boto3 client for this call."""
        if config and (config.aws_access_key_id and config.aws_secret_access_key):
            return self._create_client(config)
        if self._default_client:
            return self._default_client
        raise AthenaError(
            "No AWS credentials available. Either provide per-call credentials "
            "or start the server without --no-default-creds.",
            "NO_CREDENTIALS",
        )

    async def execute_query(
        self, request: QueryRequest, call_config: Optional[Config] = None
    ) -> Union[QueryResult, str]:
        """
        Execute a query and return results or execution ID if timeout.

        Returns:
            QueryResult if completed within timeout, otherwise query_execution_id string
        """
        config = call_config or self.config
        client = self._get_client(call_config)

        logger.info(f"Executing query in database: {request.database}")
        logger.debug(f"Query: {request.query[:200]}...")  # Log first 200 chars

        try:
            # Validate and sanitize inputs
            QueryValidator.validate_query(request.query)
            sanitized_database = QueryValidator.sanitize_identifier(request.database)

            # Start query execution
            start_params = {
                "QueryString": request.query,
                "QueryExecutionContext": {"Database": sanitized_database},
                "ResultConfiguration": {"OutputLocation": config.s3_output_location},
            }

            if config.athena_workgroup:
                start_params["WorkGroup"] = config.athena_workgroup
                logger.debug(f"Using workgroup: {config.athena_workgroup}")

            response = await asyncio.to_thread(
                client.start_query_execution, **start_params
            )
            query_execution_id = response["QueryExecutionId"]

            logger.info(f"Started query execution: {query_execution_id}")

            # Wait for completion with timeout
            if await self._wait_for_completion(query_execution_id, client, config):
                logger.info(f"Query completed successfully: {query_execution_id}")
                query_result: QueryResult = await self.get_query_results(
                    query_execution_id, request.max_rows, call_config
                )
                return query_result
            else:
                # Timeout - return execution ID for later retrieval
                logger.warning(f"Query timed out: {query_execution_id}")
                execution_id: str = query_execution_id
                return execution_id

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            logger.error(f"AWS Athena error: {error_code} - {str(e)}")
            raise AthenaError(str(e), error_code)
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {str(e)}")
            raise

    async def get_query_status(
        self, query_execution_id: str, call_config: Optional[Config] = None
    ) -> QueryStatus:
        """Get the status of a query execution."""
        client = self._get_client(call_config)
        logger.debug(f"Getting status for query: {query_execution_id}")

        try:
            response = await asyncio.to_thread(
                client.get_query_execution, QueryExecutionId=query_execution_id
            )
            execution = response.get("QueryExecution", {})

            status = execution.get("Status", {})
            statistics = execution.get("Statistics", {})

            query_status = QueryStatus(
                query_execution_id=query_execution_id,
                state=QueryState(status.get("State", "UNKNOWN")),
                state_change_reason=status.get("StateChangeReason"),
                bytes_scanned=statistics.get("DataScannedInBytes", 0),
                execution_time_ms=statistics.get("EngineExecutionTimeInMillis", 0),
            )

            logger.debug(f"Query {query_execution_id} status: {query_status.state}")
            return query_status

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            logger.error(f"Error getting query status: {error_code} - {str(e)}")
            raise AthenaError(str(e), error_code, query_execution_id)

    async def get_query_results(
        self, query_execution_id: str, max_rows: int = 1000, call_config: Optional[Config] = None
    ) -> QueryResult:
        """Get results for a completed query."""
        client = self._get_client(call_config)
        logger.info(f"Getting results for query: {query_execution_id}, max_rows: {max_rows}")

        try:
            # Check status first
            status = await self.get_query_status(query_execution_id, call_config)

            if status.state in [QueryState.RUNNING, QueryState.QUEUED]:
                raise AthenaError("Query is still running", "QUERY_RUNNING", query_execution_id)

            if status.state == QueryState.FAILED:
                reason = status.state_change_reason or "Query failed"
                logger.error(f"Query failed: {query_execution_id} - {reason}")
                raise AthenaError(reason, "QUERY_FAILED", query_execution_id)

            if status.state != QueryState.SUCCEEDED:
                logger.error(f"Query in unexpected state: {status.state}")
                raise AthenaError(
                    f"Query in unexpected state: {status.state}",
                    "UNEXPECTED_STATE",
                    query_execution_id,
                )

            # Get results with pagination support
            # Athena API returns max 1000 rows per call, so paginate if needed
            columns = []
            rows = []
            next_token = None
            is_first_page = True

            while len(rows) < max_rows:
                # Account for header row on first page, but never exceed API limit of 1000
                page_size = min(max_rows - len(rows), 999 if is_first_page else 1000)
                get_params = {
                    "QueryExecutionId": query_execution_id,
                    "MaxResults": page_size + (1 if is_first_page else 0),  # +1 for header on first page
                }
                if next_token:
                    get_params["NextToken"] = next_token

                response = await asyncio.to_thread(
                    client.get_query_results, **get_params
                )

                result_set = response.get("ResultSet", {})

                # Extract columns from first page only
                if is_first_page:
                    column_info = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
                    columns = [col.get("Name", "") for col in column_info]

                # Extract rows (skip header on first page for SELECT queries)
                rows_data = result_set.get("Rows", [])
                start_index = 1 if is_first_page and len(rows_data) > 0 and columns else 0

                for row_data in rows_data[start_index:]:
                    if len(rows) >= max_rows:
                        break
                    row = {}
                    data_list = row_data.get("Data", [])
                    for i, data in enumerate(data_list):
                        if i < len(columns):
                            row[columns[i]] = data.get("VarCharValue")
                    rows.append(row)

                is_first_page = False
                next_token = response.get("NextToken")
                if not next_token:
                    break

            result = QueryResult(
                query_execution_id=query_execution_id,
                columns=columns,
                rows=rows,
                row_count=len(rows),
                bytes_scanned=status.bytes_scanned,
                execution_time_ms=status.execution_time_ms,
            )

            logger.info(f"Retrieved {len(rows)} rows for query: {query_execution_id}")
            return result

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            logger.error(f"Error getting query results: {error_code} - {str(e)}")
            raise AthenaError(str(e), error_code, query_execution_id)

    async def list_databases(self, call_config: Optional[Config] = None) -> List[str]:
        """List all databases in the Athena catalog."""
        client = self._get_client(call_config)
        logger.info("Listing databases")

        try:
            def _list_databases():
                databases = []
                paginator = client.get_paginator("list_databases")
                for page in paginator.paginate(CatalogName="AwsDataCatalog"):
                    for db in page.get("DatabaseList", []):
                        databases.append(db.get("Name", ""))
                return sorted(databases)

            databases = await asyncio.to_thread(_list_databases)
            logger.info(f"Found {len(databases)} databases")
            return databases

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            logger.error(f"Error listing databases: {error_code} - {str(e)}")
            raise AthenaError(str(e), error_code)

    async def list_tables(
        self, database: str, call_config: Optional[Config] = None
    ) -> DatabaseInfo:
        """List all tables in a database."""
        logger.info(f"Listing tables in database: {database}")

        sanitized_database = QueryValidator.sanitize_identifier(database)

        request = QueryRequest(database=sanitized_database, query="SHOW TABLES", max_rows=1000)

        result = await self.execute_query(request, call_config)

        if isinstance(result, str):
            # Query timed out
            logger.error(f"SHOW TABLES query timed out for database: {database}")
            raise AthenaError("SHOW TABLES query timed out", "TIMEOUT", result)

        # Extract table names
        tables = [row.get("tab_name", "") for row in result.rows if "tab_name" in row]

        database_info = DatabaseInfo(
            database=sanitized_database, tables=tables, table_count=len(tables)
        )

        logger.info(f"Found {len(tables)} tables in database: {database}")
        return database_info

    async def describe_table(
        self, database: str, table_name: str, call_config: Optional[Config] = None
    ) -> TableInfo:
        """Get schema information for a specific table."""
        logger.info(f"Describing table: {database}.{table_name}")

        sanitized_database = QueryValidator.sanitize_identifier(database)
        sanitized_table = QueryValidator.sanitize_identifier(table_name)

        request = QueryRequest(
            database=sanitized_database, query=f"DESCRIBE {sanitized_table}", max_rows=1000
        )

        result = await self.execute_query(request, call_config)

        if isinstance(result, str):
            # Query timed out
            logger.error(f"DESCRIBE query timed out for table: {database}.{table_name}")
            raise AthenaError(f"DESCRIBE {table_name} query timed out", "TIMEOUT", result)

        # Extract column information
        columns = []
        for row in result.rows:
            columns.append(
                {
                    "name": row.get("col_name", ""),
                    "type": row.get("data_type", ""),
                    "comment": row.get("comment", ""),
                }
            )

        table_info = TableInfo(
            database=sanitized_database, table_name=sanitized_table, columns=columns
        )

        logger.info(f"Described table {database}.{table_name} with {len(columns)} columns")
        return table_info

    async def _wait_for_completion(
        self, query_execution_id: str, client=None, config: Optional[Config] = None
    ) -> bool:
        """
        Wait for query completion with timeout.

        Returns:
            True if completed successfully, False if timed out
        """
        cfg = config or self.config
        if client is None:
            client = self._get_client()
        timeout_seconds = cfg.timeout_seconds
        start_time = time.time()

        logger.debug(
            f"Waiting for query completion: {query_execution_id}, timeout: {timeout_seconds}s"
        )

        while time.time() - start_time < timeout_seconds:
            try:
                response = await asyncio.to_thread(
                    client.get_query_execution, QueryExecutionId=query_execution_id
                )
                state = response.get("QueryExecution", {}).get("Status", {}).get("State")

                if state == QueryState.SUCCEEDED:
                    logger.debug(f"Query completed successfully: {query_execution_id}")
                    return True

                if state in [QueryState.FAILED, QueryState.CANCELLED]:
                    reason = (
                        response.get("QueryExecution", {})
                        .get("Status", {})
                        .get("StateChangeReason", "Query failed")
                    )
                    logger.error(f"Query failed: {query_execution_id} - {reason}")
                    raise AthenaError(reason, "QUERY_FAILED", query_execution_id)

                # Wait before checking again
                await asyncio.sleep(1)

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
                logger.error(f"Error waiting for query completion: {error_code} - {str(e)}")
                raise AthenaError(str(e), error_code, query_execution_id)

        # Timeout reached
        logger.warning(f"Query timed out after {timeout_seconds}s: {query_execution_id}")
        return False
