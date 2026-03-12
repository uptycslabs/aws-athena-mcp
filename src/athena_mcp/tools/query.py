"""
Query execution tools for AWS Athena MCP Server.

Simple tools for executing queries and getting results.
"""

import json
from typing import TYPE_CHECKING, Optional

from ..athena import AthenaClient, AthenaError
from ..config import Config
from ..models import QueryRequest, QueryResult

if TYPE_CHECKING:
    from fastmcp import FastMCP


def register_query_tools(mcp: "FastMCP", athena_client: AthenaClient, config: Config) -> None:
    """Register query-related MCP tools."""

    def _build_call_config(
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        s3_output_location: Optional[str] = None,
        athena_workgroup: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Optional[Config]:
        """Build per-call config if credentials or overrides are provided."""
        has_creds = aws_access_key_id and aws_secret_access_key
        has_region = region and region != config.aws_region
        has_overrides = s3_output_location or athena_workgroup or timeout_seconds

        if has_creds or has_region or has_overrides:
            return config.with_overrides(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region=region,
                s3_output_location=s3_output_location,
                athena_workgroup=athena_workgroup,
                timeout_seconds=timeout_seconds,
            )
        return None

    @mcp.tool()
    async def run_query(
        account_id: str,
        region: str,
        database: str,
        query: str,
        max_rows: int = 1000,
        s3_output_location: Optional[str] = None,
        athena_workgroup: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        Execute a SQL query against AWS Athena.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)
            database: The Athena database to query
            query: SQL query to execute
            max_rows: Maximum number of rows to return (1-10000)
            s3_output_location: S3 path for query results (overrides server default)
            athena_workgroup: Athena workgroup name (overrides server default)
            timeout_seconds: Query timeout in seconds (overrides server default)

        Returns:
            JSON string with query results or execution ID if timeout
        """
        try:
            # Validate inputs
            if not database.strip():
                raise ValueError("Database name cannot be empty")
            if not query.strip():
                raise ValueError("Query cannot be empty")
            if max_rows < 1 or max_rows > 10000:
                raise ValueError("max_rows must be between 1 and 10000")

            call_config = _build_call_config(
                region=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                s3_output_location=s3_output_location,
                athena_workgroup=athena_workgroup,
                timeout_seconds=timeout_seconds,
            )

            request = QueryRequest(database=database, query=query, max_rows=max_rows)

            result = await athena_client.execute_query(request, call_config)

            if isinstance(result, QueryResult):
                return json.dumps(result.dict(), indent=2)
            else:
                # Timeout - return execution ID
                return json.dumps(
                    {
                        "query_execution_id": result,
                        "status": "timeout",
                        "message": "Query timed out, use get_status to check progress",
                    },
                    indent=2,
                )

        except AthenaError as e:
            return json.dumps(
                {"error": e.message, "code": e.code, "query_execution_id": e.query_execution_id},
                indent=2,
            )
        except Exception as e:
            return json.dumps({"error": str(e), "code": "INVALID_REQUEST"}, indent=2)

    @mcp.tool()
    async def get_status(
        account_id: str,
        region: str,
        query_execution_id: str,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        Get the current status of a query execution.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)
            query_execution_id: The query execution ID

        Returns:
            JSON string with status information
        """
        try:
            if not query_execution_id.strip():
                raise ValueError("Query execution ID cannot be empty")

            call_config = _build_call_config(
                region, aws_access_key_id, aws_secret_access_key, aws_session_token
            )

            status = await athena_client.get_query_status(query_execution_id, call_config)
            return json.dumps(status.dict(), indent=2)

        except AthenaError as e:
            return json.dumps(
                {"error": e.message, "code": e.code, "query_execution_id": e.query_execution_id},
                indent=2,
            )
        except Exception as e:
            return json.dumps({"error": str(e), "code": "INVALID_REQUEST"}, indent=2)

    @mcp.tool()
    async def get_result(
        account_id: str,
        region: str,
        query_execution_id: str,
        max_rows: int = 1000,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        Get results for a completed query.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)
            query_execution_id: The query execution ID
            max_rows: Maximum number of rows to return (1-10000)

        Returns:
            JSON string with query results
        """
        try:
            if not query_execution_id.strip():
                raise ValueError("Query execution ID cannot be empty")
            if max_rows < 1 or max_rows > 10000:
                raise ValueError("max_rows must be between 1 and 10000")

            call_config = _build_call_config(
                region, aws_access_key_id, aws_secret_access_key, aws_session_token
            )

            result = await athena_client.get_query_results(
                query_execution_id, max_rows, call_config
            )
            return json.dumps(result.dict(), indent=2)

        except AthenaError as e:
            return json.dumps(
                {"error": e.message, "code": e.code, "query_execution_id": e.query_execution_id},
                indent=2,
            )
        except Exception as e:
            return json.dumps({"error": str(e), "code": "INVALID_REQUEST"}, indent=2)
