"""Configuration helpers for official S3 plugin surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class S3ConnectionConfig:
    """Connection settings for S3 source and sink components."""

    region_name: str | None = None
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    addressing_style: Literal["auto", "path", "virtual"] = "auto"


def coerce_connection_config(
    *,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    addressing_style: Literal["auto", "path", "virtual"] = "auto",
    connection: S3ConnectionConfig | None = None,
) -> S3ConnectionConfig:
    if connection is None:
        return S3ConnectionConfig(
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            addressing_style=addressing_style,
        )
    if (
        any(
            value is not None
            for value in (
                region_name,
                endpoint_url,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
            )
        )
        or addressing_style != "auto"
    ):
        raise ValueError(
            "Pass either connection=S3ConnectionConfig(...) or individual S3 "
            "connection settings, not both."
        )
    return connection


def build_s3_client(connection: S3ConnectionConfig) -> Any:
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise ImportError(
            "S3 plugins require boto3. Install via: pip install 'agora-etl-plugins[s3]'"
        ) from None

    client_kwargs: dict[str, Any] = {
        "service_name": "s3",
    }
    if connection.region_name is not None:
        client_kwargs["region_name"] = connection.region_name
    if connection.endpoint_url is not None:
        client_kwargs["endpoint_url"] = connection.endpoint_url
    if connection.aws_access_key_id is not None:
        client_kwargs["aws_access_key_id"] = connection.aws_access_key_id
    if connection.aws_secret_access_key is not None:
        client_kwargs["aws_secret_access_key"] = connection.aws_secret_access_key
    if connection.aws_session_token is not None:
        client_kwargs["aws_session_token"] = connection.aws_session_token
    if connection.addressing_style != "auto":
        client_kwargs["config"] = Config(s3={"addressing_style": connection.addressing_style})
    session = boto3.session.Session()
    return session.client(**client_kwargs)
