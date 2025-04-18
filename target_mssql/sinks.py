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
import os
from singer_sdk.helpers._conformers import replace_leading_digit, snakecase

from target_mssql.connector import mssqlConnector

import pandas as pd
import subprocess
import collections
import hashlib

class mssqlSink(SQLSink):
    """mssql target sink class."""
    connector_class = mssqlConnector
    dropped_tables = dict()


    # Maintaining a dict that hold the order of columns in all streams being loaded.
    # This is a cache so we do not need to recreate it for each batch.
    table_column_order = dict()

    # This dict will hold column name mapping for all tables from
    # conformed column name to original schema column name.
    # e.g. listId -> list_id, listId -> list___id
    conform_column_map = dict()

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
    def connection(self) -> sqlalchemy.engine.Connection:
        """Get or set the SQLAlchemy connection for this sink.

        Returns:
            A connection object.
        """
        return self.connector.get_connection()

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

        self.logger.info(f"Inserting to temp table {full_table_name}")

        # create order map only once for each table.
        if full_table_name not in self.table_column_order:
            self.table_column_order[full_table_name] = self.connector.get_column_order(full_table_name)

        # create the map only once for each table.
        if full_table_name not in self.conform_column_map:
            self.conform_column_map[full_table_name] = self.get_conform_column_map(self.schema)
        
        default_column_order = self.table_column_order[full_table_name]
        column_map = self.conform_column_map[full_table_name]
        
        columns = self.column_representation(schema)

        # temporary fix to ensure missing properties are added
        insert_records = []
        missing_db_columns = set()
        for record in records:
            insert_record = {}
            for db_column in default_column_order:
                column = columns.get(db_column, None)

                # Field is the property name is un-conformed schema.
                # This is because data.singer file records are using un-conformed schema.
                field = column_map.get(db_column, None)

                if column is None:
                    missing_db_columns.add(db_column)

                    # Use case where field map column was removed by the user.
                    insert_record[db_column] = ""
                    continue

                if "boolean" in self.schema["properties"][field]['type']:
                    # cast booleans
                    insert_record[column.name] = "1" if record.get(field) else "0"
                elif record.get(field) and self.schema["properties"][field].get("format") == "date-time":
                    insert_record[column.name] = record.get(field).strftime('%Y-%m-%d %H:%M:%S.%f')
                else:
                    insert_record[column.name] = record.get(field)

            insert_records.append(insert_record)

        for db_column in missing_db_columns:
            self.logger.info(
                f"Column {db_column} exists in table {full_table_name} but missing in singer SCHEMA, user must have deselected this column")

        database = self.config.get("database")
        db_schema = full_table_name.split(".")[0] if "." in full_table_name else "dbo"
        table_name = full_table_name.split(".")[-1]
        host = self.config.get("host")
        user = self.config.get("user")
        password = self.config.get("password")
        port = self.config.get("port")

        # build the dataframe
        df = pd.DataFrame(insert_records)
        df = df.replace(r"[\n\r\t]", " ", regex=True)
        df.to_csv(f"{table_name}.csv", index=False, header=False, sep="\t")

        # run bcp
        bcp = "/opt/mssql-tools/bin/bcp" if os.environ.get("JOB_ROOT") else "bcp"
        db = f'"[{database}].[{db_schema}].[{table_name}]"'
        bcp_cmd = f'{bcp} {db} in {table_name}.csv -S "{host},{port}" -U "{user}" -P "{password}" -c -t"\t"  -e "error_log.txt"'
        bcp_log = f'{bcp} {db} in {table_name}.csv -S "{host},{port}" -U "[user]" -P "[password]" -c -t"\t"  -e "error_log.txt"'
        self.logger.info( f"BCP Command: {bcp_log}")
        result = subprocess.run(
            bcp_cmd,
            shell=True, capture_output=True, text=True
        )
        
        self.logger.info(result.stdout)
        if "Login failed" in result.stdout or "Login timeout" in result.stdout:
            raise Exception(result.stdout)

        if result.stderr:
            self.logger.error(result.stderr)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def get_conform_column_map(self, schema: dict) -> dict:
        """Return dict that maps conformed db col names to un-conformed column names.

        Args:
            schema: JSON schema dictionary.

        Returns:
            a dict that maps conformed db col names to un-conformed column names.
        """
        conformed_schema = copy(schema)
        conformed_names = [self.conform_name(key) for key in conformed_schema["properties"].keys()]
        duplicates = [item for item, count in collections.Counter(conformed_names).items() if count > 1]

        columns = {}
        for key in conformed_schema["properties"].keys():
            conformed_name = self.conform_name(key)
            if conformed_name in duplicates:
                hash = self.hash_name(key)
                new_key = f"{conformed_name}_{hash}"
                columns[new_key] = key
            else:
                columns[conformed_name] = key
        
        return columns


    def column_representation(
            self,
            schema: dict,
    ) -> Dict[str, Column]:
        """Returns a dictionary of SQLAlchemy column representations for the current schema."""
        columns: Dict[str, Column] = {}
        conformed_properties = self.conform_schema(schema)["properties"]

        for property_name, property_jsonschema in conformed_properties.items():
            columns[property_name] = Column(
                property_name,
                self.connector.to_sql_type(property_jsonschema),
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
            parts = self.full_table_name.split('.')
            db_schema = parts[0] + "." if "." in self.full_table_name else ""
            temp_table = "temp_" + parts[-1]

            self.bulk_insert_records(
                full_table_name=f"{db_schema}{temp_table}",
                schema=conformed_schema,
                records=context["records"],
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=f"{db_schema}{temp_table}",
                to_table_name=f"{self.full_table_name}",
                schema=conformed_schema,
                join_keys=self.key_properties,
            )

            self.logger.info(f"Dropping temp table as batch is done {self.full_table_name}")
            self.connector.drop_temp_table_from_table(
                temp_table=f"{db_schema}{temp_table}"
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

        to_schema = to_table_name.split(".")[0] if "." in to_table_name else "dbo"
        to_table = to_table_name.split(".")[-1]

        merge_sql = f"""
            MERGE INTO {to_schema}.[{to_table}] AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    { update_stmt }
            WHEN NOT MATCHED THEN
                INSERT ({", ".join([f"[{key}]" for key in schema["properties"].keys()])})
                VALUES ({", ".join([f"temp.[{key}]" for key in schema["properties"].keys()])});
        """

        self.logger.info(f"Merge SQL {merge_sql}")

        def do_merge(conn, merge_sql, is_check_string_key_properties):
            if is_check_string_key_properties:
                conn.execute(f"SET IDENTITY_INSERT {to_schema}.[{to_table}] ON")
            
            conn.execute(merge_sql)

            if is_check_string_key_properties:
                conn.execute(f"SET IDENTITY_INSERT {to_schema}.[{to_table}] OFF")

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
    
    def hash_name(self, name):
        return hashlib.md5(name.encode()).hexdigest()

    def conform_schema(self, schema: dict) -> dict:
        conformed_schema = copy(schema)
        conformed_property_names = {
            key: self.conform_name(key) for key in conformed_schema["properties"].keys()
        }

        duplicates = [item for item, count in collections.Counter(conformed_property_names.values()).items() if count > 1]
        
        for key,value in conformed_property_names.items():
            if value in duplicates:
                hash = self.hash_name(key)
                new_name = f"{value}_{hash}"
                conformed_property_names[key] = new_name

        self._check_conformed_names_not_duplicated(conformed_property_names)
        conformed_schema["properties"] = {
            conformed_property_names[key]: value
            for key, value in conformed_schema["properties"].items()
        }
        return conformed_schema
