"""
Schema discovery tools for AWS Athena MCP Server.

Simple tools for discovering database and table schemas.
"""

import json
from typing import TYPE_CHECKING, Optional

from ..athena import AthenaClient, AthenaError
from ..config import Config

if TYPE_CHECKING:
    from fastmcp import FastMCP


def register_schema_tools(mcp: "FastMCP", athena_client: AthenaClient, config: Config) -> None:
    """Register schema-related MCP tools."""

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
    async def list_databases(
        account_id: str,
        region: str,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        List all databases in the Athena catalog.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)

        Returns:
            JSON string with list of databases
        """
        try:
            call_config = _build_call_config(
                region, aws_access_key_id, aws_secret_access_key, aws_session_token
            )

            databases = await athena_client.list_databases(call_config)
            return json.dumps(
                {
                    "databases": databases,
                    "database_count": len(databases),
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
    async def list_tables(
        account_id: str,
        region: str,
        database: str,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        List all tables in the specified Athena database.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)
            database: The Athena database to list tables from

        Returns:
            JSON string with list of tables
        """
        try:
            if not database.strip():
                raise ValueError("Database name cannot be empty")

            call_config = _build_call_config(
                region, aws_access_key_id, aws_secret_access_key, aws_session_token
            )

            database_info = await athena_client.list_tables(database, call_config)
            return json.dumps(database_info.dict(), indent=2)

        except AthenaError as e:
            return json.dumps(
                {"error": e.message, "code": e.code, "query_execution_id": e.query_execution_id},
                indent=2,
            )
        except Exception as e:
            return json.dumps({"error": str(e), "code": "INVALID_REQUEST"}, indent=2)

    @mcp.tool()
    async def describe_table(
        account_id: str,
        region: str,
        database: str,
        table_name: str,
        profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> str:
        """
        Get detailed schema information for a specific table.

        Args:
            account_id: AWS account ID for the target account
            region: AWS region name (e.g., us-east-1, eu-west-1)
            database: The Athena database containing the table
            table_name: The name of the table to describe

        Returns:
            JSON string with table schema information
        """
        try:
            if not database.strip():
                raise ValueError("Database name cannot be empty")
            if not table_name.strip():
                raise ValueError("Table name cannot be empty")

            call_config = _build_call_config(
                region, aws_access_key_id, aws_secret_access_key, aws_session_token
            )

            table_info = await athena_client.describe_table(database, table_name, call_config)
            return json.dumps(table_info.dict(), indent=2)

        except AthenaError as e:
            return json.dumps(
                {"error": e.message, "code": e.code, "query_execution_id": e.query_execution_id},
                indent=2,
            )
        except Exception as e:
            return json.dumps({"error": str(e), "code": "INVALID_REQUEST"}, indent=2)
