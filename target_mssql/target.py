"""mssql target class."""

from __future__ import annotations
import logging

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_mssql.sinks import mssqlSink


class Targetmssql(SQLTarget):
    """Sample target for mssql."""

    name = "target-mssql"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string",
        ),
    ).to_dict()

    default_sink_class = mssqlSink

    def __init__(self, *args, **kwargs):
        """Initialize the target and configure logger to not add timestamps."""
        super().__init__(*args, **kwargs)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            force=True,
        )


if __name__ == "__main__":
    Targetmssql.cli()
