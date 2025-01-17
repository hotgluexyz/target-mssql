"""mssql target sink class, which handles writing streams."""
from __future__ import annotations
from target_mssql.timer import Timer
from datetime import datetime

from typing import Any, Dict, Iterable, List, Optional, Union
from copy import copy
import sqlalchemy
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column
from textwrap import dedent
import re
from singer_sdk.helpers._conformers import replace_leading_digit, snakecase

from target_mssql.connector import mssqlConnector


class mssqlSink(SQLSink):
    """mssql target sink class."""
    connector_class = mssqlConnector
    dropped_tables = dict()
    max_size = 10_000

    # Copied purely to help with type hints
    @property
    def connector(self) -> mssqlConnector:
        """The connector object.
        Returns:
            The connector object.
        """
        return self._connector

    @property
    def schema_name(self) -> Optional[str]:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """

        default_target_schema = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        if default_target_schema:
            return default_target_schema.replace("-", "_")

        if len(parts) in {2, 3}:
            # Stream name is a two-part or three-part identifier.
            # Use the second-to-last part as the schema name.
            stream_schema = self.conform_name(parts[-2], "schema")

            if stream_schema == "public":
                return "dbo"
            else:
                return stream_schema

        # Schema name not detected.
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and return a modified result.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        Returns:
            A new, processed record.
        """
        keys = record.keys()
        for key in keys:
            if type(record[key]) is list:
                record[key] = str(record[key])
            if isinstance(record[key], dict):
                record[key] = str(record[key])
        return record

    def check_string_key_properties(self):
        isnumeric = True
        if self.key_properties:
            schema = self.conform_schema(self.schema)
            for prop in self.key_properties:
                # prop = self.conform_name(prop)
                isnumeric = ("string" not in schema['properties'][prop]['type']) and isnumeric
            
        return self.key_properties and isnumeric

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.
        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        schema = self.conform_schema(schema)
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.info("Inserting with SQL: %s", insert_sql)

        columns = self.column_representation(schema)

        with Timer(self.logger, f"Create record batch!! Table {full_table_name}"):
            # temporary fix to ensure missing properties are added
            insert_records = []
            for record in records:
                insert_record = {}
                for column, field in zip(columns, self.schema["properties"].keys()):
                    if isinstance(record.get(field), bool):
                        insert_record[column.name] = 1 if record.get(field) == True else 0
                    elif isinstance(record.get(field), datetime):
                        insert_record[column.name] = record.get(field).strftime('%Y-%m-%d')
                    else:
                        insert_record[column.name] = record.get(field)
                insert_records.append(insert_record)

        if self.check_string_key_properties():
           self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } ON")

        with Timer(self.logger, f"Load records in database!! Table {full_table_name}; Records count {len(insert_records)}"):
            self.connection.execute(insert_sql, insert_records)

        if self.check_string_key_properties():
            self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } OFF")

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created

        conformed_schema = self.conform_schema(self.schema)

        if self.key_properties:
            self.logger.info(f"Preparing table {self.full_table_name}")

            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=conformed_schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
            )
            # self.alter_varchar_columns(self.full_table_name, conformed_schema)
            # Create a temp table (Creates from the table above)
            self.logger.info(f"Creating temp table {self.full_table_name}")
            self.connector.create_temp_table_from_table(
                from_table_name=self.full_table_name
            )

            # Insert into temp table
            self.logger.info("Inserting into temp table")
            self.bulk_insert_records(
                full_table_name=f"TMP_{self.full_table_name.split('.')[-1]}",
                schema=conformed_schema,
                records=context["records"],
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=f"TMP_{self.full_table_name.split('.')[-1]}",
                to_table_name=f"{self.full_table_name}",
                schema=conformed_schema,
                join_keys=self.key_properties,
            )

            self.logger.info(f"Dropping temp table as batch is done {self.full_table_name}")
            self.connector.drop_temp_table_from_table(
                from_table_name=self.full_table_name
            )
        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=conformed_schema,
                records=context["records"],
            )

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
    ) -> Optional[int]:
        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22

        schema = self.conform_schema(schema)

        join_condition = " and ".join(
            [f"temp.[{key}] = target.[{key}]" for key in join_keys]
        )

        update_stmt = ", ".join(
            [
                f"target.[{key}] = temp.[{key}]"
                for key in schema["properties"].keys()
                if key not in join_keys
            ]
        )  # noqa

        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    { update_stmt }
            WHEN NOT MATCHED THEN
                INSERT ({", ".join([f"[{key}]" for key in schema["properties"].keys()])})
                VALUES ({", ".join([f"temp.[{key}]" for key in schema["properties"].keys()])});
        """

        def do_merge(conn, merge_sql, is_check_string_key_properties):
            if is_check_string_key_properties:
                conn.execute(f"SET IDENTITY_INSERT { to_table_name } ON")
            
            conn.execute(merge_sql)

            if is_check_string_key_properties:
                conn.execute(f"SET IDENTITY_INSERT { to_table_name } OFF")

        is_check_string_key_properties = self.check_string_key_properties()
        self.connection.transaction(do_merge, merge_sql, is_check_string_key_properties)

    def conform_schema_new(self, schema: dict) -> dict:
        """Return schema dictionary with property names conformed.

        Args:
            schema: JSON schema dictionary.

        Returns:
            A schema dictionary with the property names conformed.
        """
        conformed_schema = copy(schema)
        conformed_property_names = {
            key: self.conform_name_new(key) for key in conformed_schema["properties"].keys()
        }
        self._check_conformed_names_not_duplicated(conformed_property_names)
        conformed_schema["properties"] = {
            conformed_property_names[key]: value
            for key, value in conformed_schema["properties"].items()
        }
        return conformed_schema

    def bracket_names(self, name: str) -> str:
        return f"[{name}]"
    
    def unbracket_names(self, name: str) -> str:
        if self.is_bracketed(name):
            return name.replace("[", "").replace("]", "")
        return name
    
    def is_bracketed(self, name: str) -> bool:
        return name.startswith("[") and name.endswith("]")
    
    def is_protected_name(self, name: str) -> bool:
        mssql_reserved_keywords = ["ADD","EXTERNAL","PROCEDURE","ALL","FETCH","PUBLIC","ALTER","FILE","RAISERROR","AND","FILLFACTOR","READ","ANY","FOR","READTEXT","AS","FOREIGN","RECONFIGURE","ASC","FREETEXT","REFERENCES","AUTHORIZATION","FREETEXTTABLE","REPLICATION","BACKUP","FROM","RESTORE","BEGIN","FULL","RESTRICT","BETWEEN","FUNCTION","RETURN","BREAK","GOTO","REVERT","BROWSE","GRANT","REVOKE","BULK","GROUP","RIGHT","BY","HAVING","ROLLBACK","CASCADE","HOLDLOCK","ROWCOUNT","CASE","IDENTITY","ROWGUIDCOL","CHECK","IDENTITY_INSERT","RULE","CHECKPOINT","IDENTITYCOL","SAVE","CLOSE","IF","SCHEMA","CLUSTERED","IN","SECURITYAUDIT","COALESCE","INDEX","SELECT","COLLATE","INNER","SEMANTICKEYPHRASETABLE","COLUMN","INSERT","SEMANTICSIMILARITYDETAILSTABLE","COMMIT","INTERSECT","SEMANTICSIMILARITYTABLE","COMPUTE","INTO","SESSION_USER","CONSTRAINT","IS","SET","CONTAINS","JOIN","SETUSER","CONTAINSTABLE","KEY","SHUTDOWN","CONTINUE","KILL","SOME","CONVERT","LEFT","STATISTICS","CREATE","LIKE","SYSTEM_USER","CROSS","LINENO","TABLE","CURRENT","LOAD","TABLESAMPLE","CURRENT_DATE","MERGE","TEXTSIZE","CURRENT_TIME","NATIONAL","THEN","CURRENT_TIMESTAMP","NOCHECK","TO","CURRENT_USER","NONCLUSTERED","TOP","CURSOR","NOT","TRAN","DATABASE","NULL","TRANSACTION","DBCC","NULLIF","TRIGGER","DEALLOCATE","OF","TRUNCATE","DECLARE","OFF","TRY_CONVERT","DEFAULT","OFFSETS","TSEQUAL","DELETE","ON","UNION","DENY","OPEN","UNIQUE","DESC","OPENDATASOURCE","UNPIVOT","DISK","OPENQUERY","UPDATE","DISTINCT","OPENROWSET","UPDATETEXT","DISTRIBUTED","OPENXML","USE","DOUBLE","OPTION","USER","DROP","OR","VALUES","DUMP","ORDER","VARYING","ELSE","OUTER","VIEW","END","OVER","WAITFOR","ERRLVL","PERCENT","WHEN","ESCAPE","PIVOT","WHERE","EXCEPT","PLAN","WHILE","EXEC","PRECISION","WITH","EXECUTE","PRIMARY","WITHIN GROUP","EXISTS","PRINT","WRITETEXT","EXIT","PROC"]
        return name.upper() in mssql_reserved_keywords
    
    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system.
        Transforms names to snake case, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.
        """
        # strip non-alphanumeric characters, keeping - . _ and spaces
        name = re.sub(r"[^a-zA-Z0-9_\-\.\s]", "", name)
        # convert to snakecase
        if name.isupper():
            name = name.lower()

        name = snakecase(name)
        # replace leading digit
        return replace_leading_digit(name)

    def conform_name_new(self, name: str, object_type: Optional[str] = None) -> str:
        name = super().conform_name(name, object_type)
        if self.is_protected_name(name):
            return self.bracket_names(name)
        return name
    
    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ):
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        property_names = list(self.conform_schema_new(schema)["properties"].keys())
        statement = dedent(
            f"""\
            INSERT INTO {full_table_name}
            ({", ".join(property_names)})
            VALUES ({", ".join([f":{self.unbracket_names(name)}" for name in property_names])})
            """
        )
        return statement.rstrip()
    
    def alter_varchar_columns(self, full_table_name: str, schema: dict):
        for key, value in schema["properties"].items():
            if key in self.key_properties:
                continue
            if value.get("type") == "string" or set(value.get("type")) == {"string", "null"}:
                self.connection.execute(f"ALTER TABLE {full_table_name} ALTER COLUMN {key} VARCHAR(MAX);")
