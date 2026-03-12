"""
AWS Athena MCP Server.

Simple, clean MCP server for AWS Athena integration.
"""

import argparse
import asyncio
import json
import sys
from functools import wraps
from typing import Any, Callable, Optional, Type

from fastmcp import FastMCP

from .athena import AthenaClient, AthenaError
from .config import Config

# Parse CLI arguments
parser = argparse.ArgumentParser(
    description="AWS Athena MCP Server"
)
parser.add_argument(
    "--profile", type=str,
    help="AWS profile name to use for credentials",
)
parser.add_argument(
    "--region", type=str,
    help="AWS region name to use for API calls",
)
args, unknown = parser.parse_known_args()

# Load configuration
try:
    config = Config.from_env()
    if args.region:
        config.aws_region = args.region
    print(f"✅ Configuration loaded: {config}")
except ValueError as e:
    print(f"❌ Configuration error: {e}")
    sys.exit(1)

# Skip startup credential validation — credentials are provided
# per-call by the proxy (Juno AssumeRole) via @with_aws_config.
print("✅ Credentials will be provided per-call")

# Create MCP server
mcp: FastMCP = FastMCP(name="aws-athena-mcp", version="1.0.0")

# Capture defaults
default_profile = args.profile
default_region = args.region


def with_aws_config(
    tool_class: Type, method_name: Optional[str] = None,
) -> Callable:
    """
    Decorator that handles profile, region, and AWS credentials
    for tool functions. Creates a new instance of the specified
    tool class with the correct credentials per call.

    When aws_access_key_id/aws_secret_access_key are provided,
    they are used directly (e.g., from Juno's AssumeRole).
    Otherwise, falls back to profile/region or default chain.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                # Pop credential params - used by proxy, not tool
                kwargs.pop("account_id", None)
                profile = (
                    kwargs.pop("profile", None) or default_profile
                )
                region = (
                    kwargs.pop("region", None) or default_region
                )
                access_key = kwargs.pop(
                    "aws_access_key_id", None
                )
                secret_key = kwargs.pop(
                    "aws_secret_access_key", None
                )
                session_token = kwargs.pop(
                    "aws_session_token", None
                )

                tool_instance = tool_class(
                    config,
                    profile_name=profile,
                    region_name=region,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=session_token,
                )
                target = method_name or func.__name__
                method = getattr(tool_instance, target)
                result = method(**kwargs)
                if asyncio.iscoroutine(result):
                    return await result
                return result
            except AthenaError as e:
                return json.dumps(
                    {
                        "error": e.message,
                        "code": e.code,
                        "query_execution_id": e.query_execution_id,
                    },
                    indent=2,
                )
            except Exception as e:
                return json.dumps(
                    {"error": str(e), "code": "INVALID_REQUEST"},
                    indent=2,
                )

        return wrapper
    return decorator


# ==============================
# Query Tools
# ==============================


@mcp.tool()
@with_aws_config(AthenaClient, method_name="run_query")
async def run_query(
    database: str,
    query: str,
    account_id: str = None,
    region: str = None,
    max_rows: int = 100,
    profile: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
) -> str:
    """
    Execute a SQL query against AWS Athena.

    Args:
        account_id: AWS account ID for the target account
        region: AWS region name (e.g., us-east-1, eu-west-1)
        database: The Athena database to query
        query: SQL query to execute
        max_rows: Maximum number of rows to return (1-10000)

    Returns:
        JSON string with query results or execution ID if timeout
    """
    pass


# ==============================
# Schema Tools
# ==============================


@mcp.tool()
@with_aws_config(AthenaClient, method_name="list_databases_json")
async def list_databases(
    account_id: str = None,
    region: str = None,
    profile: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
) -> str:
    """
    List all databases in the Athena catalog.

    Args:
        account_id: AWS account ID for the target account
        region: AWS region name (e.g., us-east-1, eu-west-1)

    Returns:
        JSON string with list of databases
    """
    pass


@mcp.tool()
@with_aws_config(AthenaClient, method_name="list_tables_json")
async def list_tables(
    database: str,
    account_id: str = None,
    region: str = None,
    profile: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
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
    pass


@mcp.tool()
@with_aws_config(AthenaClient, method_name="describe_table_json")
async def describe_table(
    database: str,
    table_name: str,
    account_id: str = None,
    region: str = None,
    profile: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
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
    pass


print("✅ MCP server created with tools:")
print("   • run_query - Execute SQL queries")
print("   • list_databases - List available databases")
print("   • list_tables - List database tables")
print("   • describe_table - Get table schema")


def main() -> None:
    """Main entry point for the server."""
    print("🚀 Starting AWS Athena MCP Server...")
    print("📡 Running MCP server with stdio transport")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
