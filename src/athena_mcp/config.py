"""
Configuration management for AWS Athena MCP Server.

Handles environment variables with sensible defaults and clear validation.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Configuration for AWS Athena MCP Server."""

    # Required settings
    s3_output_location: str
    aws_region: str = "us-east-1"

    # Optional settings
    athena_workgroup: Optional[str] = None
    timeout_seconds: int = 60

    # Credential settings
    no_default_creds: bool = False
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""

            # S3 output location: required from env unless --no-default-creds
        s3_output_location = os.getenv("ATHENA_S3_OUTPUT_LOCATION", "")

        # Validate S3 path format if provided
        if s3_output_location and not s3_output_location.startswith("s3://"):
            raise ValueError(
                f"ATHENA_S3_OUTPUT_LOCATION must be a valid S3 path starting with 's3://'. "
                f"Got: {s3_output_location}"
            )

        # Optional environment variables with defaults
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        athena_workgroup = os.getenv("ATHENA_WORKGROUP")

        # Parse timeout with validation
        timeout_str = os.getenv("ATHENA_TIMEOUT_SECONDS", "60")
        try:
            timeout_seconds = int(timeout_str)
            if timeout_seconds < 1:
                raise ValueError("Timeout must be at least 1 second")
        except ValueError as e:
            if "Timeout must be at least 1 second" in str(e):
                raise e
            raise ValueError(
                f"ATHENA_TIMEOUT_SECONDS must be a positive integer. Got: {timeout_str}"
            ) from e

        return cls(
            s3_output_location=s3_output_location,
            aws_region=aws_region,
            athena_workgroup=athena_workgroup,
            timeout_seconds=timeout_seconds,
        )

    def validate_aws_credentials(self) -> None:
        """Validate that AWS credentials are available."""
        if self.no_default_creds:
            # In no-default-creds mode, skip validation at startup;
            # credentials and config will be provided per-call
            return

        if not self.s3_output_location:
            raise ValueError(
                "ATHENA_S3_OUTPUT_LOCATION environment variable is required "
                "(or use --no-default-creds to provide per-call). "
                "Example: export ATHENA_S3_OUTPUT_LOCATION=s3://my-bucket/results/"
            )

        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        try:
            # Test credentials by creating a session and making a simple call
            session = boto3.Session(region_name=self.aws_region)
            client = session.client("athena")
            client.list_work_groups(MaxResults=1)
        except NoCredentialsError:
            raise ValueError(
                "AWS credentials not found. Please configure AWS credentials using one of:\n"
                "1. AWS CLI: aws configure\n"
                "2. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n"
                "3. IAM roles (for EC2/Lambda)\n"
                "4. AWS profiles: export AWS_PROFILE=your-profile\n"
                "5. Use --no-default-creds flag to accept per-call credentials"
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise ValueError(
                f"AWS credentials are invalid or insufficient permissions: {error_code}"
            )

    def with_overrides(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region: Optional[str] = None,
        s3_output_location: Optional[str] = None,
        athena_workgroup: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> "Config":
        """Create a new Config with per-call overrides."""
        return Config(
            s3_output_location=s3_output_location or self.s3_output_location,
            aws_region=region or self.aws_region,
            athena_workgroup=athena_workgroup or self.athena_workgroup,
            timeout_seconds=timeout_seconds or self.timeout_seconds,
            no_default_creds=self.no_default_creds,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def __str__(self) -> str:
        """String representation for logging (without sensitive data)."""
        return (
            f"Config(s3_output_location={self.s3_output_location}, "
            f"aws_region={self.aws_region}, "
            f"athena_workgroup={self.athena_workgroup}, "
            f"timeout_seconds={self.timeout_seconds}, "
            f"no_default_creds={self.no_default_creds})"
        )
