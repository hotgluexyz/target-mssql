from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, cast

import sqlalchemy
import urllib.parse
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.sinks import SQLConnector
from sqlalchemy.dialects import mssql
from target_mssql.metadata import write_event


class mssqlConnector(SQLConnector):
    """The connector for mssql.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.
    dropped_tables = dict()


    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.
        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.
        Returns:
            A newly created SQLAlchemy engine object.
        """
        engine = sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            pool_pre_ping=True,
            pool_recycle=1800
        )

        return engine

    def table_exists(self, full_table_name: str) -> bool:
        """Determine if the target table already exists.

        Args:
            full_table_name: the target table name.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        kwargs = dict()

        if "." in full_table_name:
            kwargs["schema"] = full_table_name.split(".")[0]
            full_table_name = full_table_name.split(".")[1]

        self.logger.info(f"Checking table exists: {full_table_name} kwrags={kwargs}")

        return cast(
            bool,
            sqlalchemy.inspect(self._engine).has_table(full_table_name, **kwargs),
        )

    def prepare_column(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:

        schema = full_table_name.split(".")[0] if "." in full_table_name else "dbo"
        table_name = full_table_name.split(".")[-1]

        """
        Ensure a column exists in the table with the correct data type.
        If the column does not exist, create it.

        :param full_table_name: The full table name (schema.table_name).
        :param column_name: The name of the column to ensure.
        :param sql_type: The SQLAlchemy type to enforce.
        """
        # Check if the column exists
        column_exists_query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name}'
          AND COLUMN_NAME = '{column_name}'
        """
        result = self.connection.execute(column_exists_query).scalar()

        if result == 0:
            # Add the column if it does not exist
            alter_sql = f"ALTER TABLE {schema}.[{table_name}] ADD [{column_name}] {sql_type.compile(dialect=mssql.dialect())}"
            with self.connection.begin():
                self.connection.execute(alter_sql)
            self.logger.info(f"Added column {column_name} to {full_table_name}.")

    from sqlalchemy import create_engine

    def get_column_order(self, full_table_name):

        schema = full_table_name.split(".")[0] if "." in full_table_name else "dbo"
        table_name = full_table_name.split(".")[-1]

        # SQL query to get the column order
        query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_SCHEMA = '{schema}' AND
            TABLE_NAME = '{table_name}'
        ORDER BY 
            ORDINAL_POSITION;
        """

        with self.connection.begin():
            result = self.connection.execute(query)

            # Fetch all the results and return the column names in order
            column_order = [row['COLUMN_NAME'] for row in result]

            # Return the column order as a list
            return column_order

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        # NOTE: Force create the table
        # TODO: remove this
        # if not self.dropped_tables.get(full_table_name, False):
        #     self.logger.info(f"Force dropping the table {full_table_name}!")
        #     with self.connection.begin():  # Starts a transaction
        #         drop_table = '.'.join([f'[{x}]' for x in full_table_name.split('.')])
        #         self.connection.execute(f"DROP TABLE IF EXISTS {drop_table};")
        #     self.dropped_tables[full_table_name] = True

        if not self.table_exists(full_table_name=full_table_name):
            self.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
            )
            return

        for property_name, property_def in schema["properties"].items():
            self.prepare_column(
                full_table_name, property_name, self.to_sql_type(property_def)
            )

    def prepare_schema(self, schema_name: str) -> None:
        """Create the target database schema.

        Args:
            schema_name: The target schema name.
        """
        schema_exists = self.schema_exists(schema_name)
        if not schema_exists:
            write_event({"event": "SCHEMA_CREATED", "name": schema_name})
            self.create_schema(schema_name)

    def create_table_with_records(
        self,
        full_table_name: Optional[str],
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            records: records to load.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        full_table_name = full_table_name or self.full_table_name
        if primary_keys is None:
            primary_keys = self.key_properties
        partition_keys = partition_keys or None

        self.connector.prepare_table(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
            schema=schema,
            as_temp_table=as_temp_table,
        )
        self.bulk_insert_records(
            full_table_name=full_table_name, schema=schema, records=records
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.
        """

        connection_url = sqlalchemy.engine.url.URL.create(
            drivername="mssql+pyodbc",
            username=config['user'],
            password=urllib.parse.quote_plus(config["password"]),
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query={
                "driver": "ODBC Driver 17 for SQL Server",  # Use Microsoft's ODBC driver
                "Encrypt": "yes",  # Ensures SSL encryption for Azure SQL
                "TrustServerCertificate": "yes",  # Prevents bypassing certificate validation
                "MARS_Connection": "Yes",
                "ConnectRetryCount": "3",
                "ConnectRetryInterval": "15"
            }
        )

        return str(connection_url)

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        meta = sqlalchemy.MetaData()
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys

            columntype = self.to_sql_type(property_jsonschema)

            if is_primary_key:
                # In MSSQL, Primary keys can not be more than 900 bytes. Setting at 255
                if isinstance(columntype, sqlalchemy.types.VARCHAR) or isinstance(columntype, sqlalchemy.types.NVARCHAR):
                    columntype = sqlalchemy.types.VARCHAR(255)
                # elif isinstance(columntype, sqlalchemy.types.BIGINT):
                #     columntype = sqlalchemy.types.INTEGER()

            columns.append(
                sqlalchemy.Column(
                    property_name,
                    columntype,
                    primary_key=is_primary_key,
                )
            )

        kwargs = dict()

        schema = full_table_name.split(".")[0] if "." in full_table_name else "dbo"
        full_table_name = full_table_name.split(".")[-1]

        kwargs["schema"] = schema

        _ = sqlalchemy.Table(full_table_name, meta, *columns, **kwargs)
        meta.create_all(self._engine)

        write_event({"event": "TABLE_CREATED", "name": full_table_name, "schema": schema})

        # self.logger.info(f"Create table with cols = {columns}")

    def merge_sql_types(  # noqa
        self, sql_types: list[sqlalchemy.types.TypeEngine]
    ) -> sqlalchemy.types.TypeEngine:  # noqa
        """Return a compatible SQL type for the selected type list.
        Args:
            sql_types: List of SQL types.
        Returns:
            A SQL type that is compatible with the input types.
        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        # Gathering Type to match variables
        # sent in _adapt_column_type
        current_type = sql_types[0]
        # sql_type = sql_types[1]

        # Getting the length of each type
        # current_type_len: int = getattr(sql_types[0], "length", 0)
        sql_type_len: int = getattr(sql_types[1], "length", 0)
        if sql_type_len is None:
            sql_type_len = 0

        # Convert the two types given into a sorted list
        # containing the best conversion classes
        sql_types = self._sort_types(sql_types)

        # If greater than two evaluate the first pair then on down the line
        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            if isinstance(generic_type, type):
                if issubclass(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                        (opt_len is None)
                        or (opt_len == 0)
                        or (current_type.length is None)
                        or (opt_len >= current_type.length)
                    ):
                        return opt
                elif isinstance(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                        (opt_len is None)
                        or (opt_len == 0)
                        or (current_type.length is None)
                        or (opt_len >= current_type.length)
                    ):
                        return opt
                # If best conversion class is equal to current type
                # return the best conversion class
                elif str(opt) == str(current_type):
                    return opt

        raise ValueError(
            f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        )

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.
        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type).split(" ")[0] == str(current_type).split(" ")[0]:
            # Nothing to do
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )
        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) }
                ALTER COLUMN { str(column_name) } { str(compatible_sql_type) }"""
            )
        except Exception as e:
            raise RuntimeError(
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            ) from e

        # self.connection.execute(
        #     sqlalchemy.DDL(
        #         "ALTER TABLE %(table)s ALTER COLUMN %(col_name)s %(col_type)s",
        #         {
        #             "table": full_table_name,
        #             "col_name": column_name,
        #             "col_type": compatible_sql_type,
        #         },
        #     )
        # )

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.
        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                sql_type,
            )
        )

        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) }
                ADD { str(create_column_clause) } """
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{create_column_clause}' "
                f"on table '{full_table_name}'."
            ) from e

    def _jsonschema_type_check(
        self, jsonschema_type: dict, type_check: tuple[str]
    ) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
            return True

        return False

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:  # noqa
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(
                        sqlalchemy.types.TypeEngine, sqlalchemy.types.DATETIME()
                    )
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

            maxlength = jsonschema_type.get("maxLength")
            return cast(
                sqlalchemy.types.TypeEngine, sqlalchemy.types.NVARCHAR(maxlength)
            )

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.BIGINT())
        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NUMERIC(29, 16))
        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(1))

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NVARCHAR())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NVARCHAR())

        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    def drop_temp_table_from_table(self, temp_table):
        """Drop the temp table from an existing table, preserving identity columns and default values."""

        try:
            self.logger.info(f"Dropping existing temp table {temp_table}")
            with self.connection.begin():  # Starts a transaction
                temp_table = '.'.join([f'[{x}]' for x in temp_table.split('.')])
                self.connection.execute(f"DROP TABLE IF EXISTS {temp_table};")
        except Exception as e:
            self.logger.info(f"No temp table to drop. Error: {e}")

    def create_temp_table_from_table(self, from_table_name):
        """Create a temp table from an existing table, preserving identity columns and default values."""
        parts = from_table_name.split(".")
        schema = parts[0] + "." if "." in from_table_name else ""
        table = "temp_" + parts[-1]

        self.drop_temp_table_from_table(f"{schema}{table}")

        # Query to get column definitions, including identity property
        get_columns_query = f"""
            SELECT 
                c.name AS COLUMN_NAME,
                t.name AS DATA_TYPE,
                c.max_length AS COLUMN_LENGTH,
                c.precision AS PRECISION_VALUE,
                c.scale AS SCALE_VALUE,
                d.definition AS COLUMN_DEFAULT,
                COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS IS_IDENTITY
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.default_constraints d ON c.default_object_id = d.object_id
            WHERE c.object_id = OBJECT_ID('{from_table_name}')
        """

        columns = self.connection.execute(get_columns_query).fetchall()
        # self.logger.info(f"Fetched columns: {columns}")

        # Construct the CREATE TABLE statement
        column_definitions = []
        for col in columns:
            col_name = col[0]
            col_type = col[1]
            col_length = col[2]
            precision_value = col[3]
            scale_value = col[4]
            col_default = f"DEFAULT {col[5]}" if col[5] else ""
            is_identity = col[6]

            identity_str = "IDENTITY(1,1)" if is_identity else ""

            # Apply length only if it's a varchar/nvarchar type
            if col_type.lower() in ["varchar", "nvarchar"]:
                col_length_str = "(MAX)" if col_length == -1 else f"({col_length})"
            else:
                col_length_str = ""

            column_definitions.append(f"[{col_name}] {col_type}{col_length_str} {identity_str} {col_default}")

        create_temp_table_sql = f"""
            CREATE TABLE {schema}{table} (
                {", ".join(column_definitions)}
            );
        """

        # self.logger.info(f"Generated SQL for temp table:\n{create_temp_table_sql}")
        self.connection.execute(create_temp_table_sql)
