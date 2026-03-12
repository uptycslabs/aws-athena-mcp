"""
AWS Athena MCP Server.

Simple, clean MCP server for AWS Athena integration.
"""

import argparse
import sys

from fastmcp import FastMCP

from .athena import AthenaClient
from .config import Config
from .tools import register_query_tools, register_schema_tools

# Parse CLI arguments
parser = argparse.ArgumentParser(description="AWS Athena MCP Server")
parser.add_argument("--profile", type=str, help="AWS profile name", default=None)
parser.add_argument("--region", type=str, help="AWS region", default=None)
parser.add_argument(
    "--no-default-creds",
    action="store_true",
    help="Disable default credential chain; require per-call credentials",
    default=False,
)
args, unknown = parser.parse_known_args()


def create_server() -> FastMCP:
    """Create and configure the AWS Athena MCP server."""

    # Load configuration
    try:
        config = Config.from_env()
        config.no_default_creds = args.no_default_creds

        # Override region from CLI if provided
        if args.region:
            config.aws_region = args.region

        print(f"✅ Configuration loaded: {config}")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)

    # Validate AWS credentials
    try:
        config.validate_aws_credentials()
        if config.no_default_creds:
            print("✅ Running in no-default-creds mode (per-call credentials required)")
        else:
            print("✅ AWS credentials validated")
    except ValueError as e:
        print(f"❌ AWS credentials error: {e}")
        sys.exit(1)

    # Create MCP server
    mcp: FastMCP = FastMCP(name="aws-athena-mcp", version="1.0.0")

    # Create Athena client
    athena_client = AthenaClient(config)

    # Register tools
    register_query_tools(mcp, athena_client, config)
    register_schema_tools(mcp, athena_client, config)

    print("✅ MCP server created with tools:")
    print("   • run_query - Execute SQL queries")
    print("   • get_status - Check query status")
    print("   • get_result - Get query results")
    print("   • list_databases - List available databases")
    print("   • list_tables - List database tables")
    print("   • describe_table - Get table schema")

    return mcp


def main() -> None:
    """Main entry point for the server."""
    print("🚀 Starting AWS Athena MCP Server...")

    # Create server
    mcp = create_server()

    # Run server (stdio transport only for simplicity)
    print("📡 Running MCP server with stdio transport")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
