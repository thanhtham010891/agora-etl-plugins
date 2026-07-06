"""Configuration helpers for official BigQuery plugin surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BigQueryConnectionConfig:
    """Connection settings for BigQuery source and sink components."""

    project: str | None = None
    location: str | None = None
    credentials_path: str | None = None
    credentials: Any | None = None


def coerce_connection_config(
    *,
    project: str | None = None,
    location: str | None = None,
    credentials_path: str | None = None,
    credentials: Any | None = None,
    connection: BigQueryConnectionConfig | None = None,
) -> BigQueryConnectionConfig:
    if connection is None:
        return BigQueryConnectionConfig(
            project=project,
            location=location,
            credentials_path=credentials_path,
            credentials=credentials,
        )
    if any(value is not None for value in (project, location, credentials_path, credentials)):
        raise ValueError(
            "Pass either connection=BigQueryConnectionConfig(...) or individual "
            "project/location/credentials settings, not both."
        )
    return connection


def build_bigquery_client(connection: BigQueryConnectionConfig) -> Any:
    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "BigQuery plugins require google-cloud-bigquery. "
            "Install via: pip install 'agora-etl-plugins[bigquery]'"
        ) from None

    client_kwargs: dict[str, Any] = {}
    if connection.project is not None:
        client_kwargs["project"] = connection.project
    if connection.location is not None:
        client_kwargs["location"] = connection.location
    if connection.credentials is not None and connection.credentials_path is not None:
        raise ValueError("Specify either credentials or credentials_path, not both.")
    if connection.credentials is not None:
        client_kwargs["credentials"] = connection.credentials
    elif connection.credentials_path is not None:
        try:
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise ImportError(
                "BigQuery service-account loading requires google-cloud-bigquery "
                "dependencies. Install via: pip install 'agora-etl-plugins[bigquery]'"
            ) from None
        client_kwargs["credentials"] = Credentials.from_service_account_file(
            str(Path(connection.credentials_path))
        )
    return bigquery.Client(**client_kwargs)


def build_bigquery_write_client(connection: BigQueryConnectionConfig) -> Any:
    try:
        from google.cloud import bigquery_storage_v1
    except ImportError:
        raise ImportError(
            "BigQuery Storage Write plugins require google-cloud-bigquery-storage. "
            "Install via: pip install 'agora-etl-plugins[bigquery]'"
        ) from None

    client_kwargs: dict[str, Any] = {}
    if connection.credentials is not None and connection.credentials_path is not None:
        raise ValueError("Specify either credentials or credentials_path, not both.")
    if connection.credentials is not None:
        client_kwargs["credentials"] = connection.credentials
    elif connection.credentials_path is not None:
        try:
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise ImportError(
                "BigQuery Storage Write service-account loading requires the "
                "bigquery plugin dependencies. Install via: "
                "pip install 'agora-etl-plugins[bigquery]'"
            ) from None
        client_kwargs["credentials"] = Credentials.from_service_account_file(
            str(Path(connection.credentials_path))
        )
    return bigquery_storage_v1.BigQueryWriteClient(**client_kwargs)
