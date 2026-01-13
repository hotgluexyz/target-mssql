# target-mssql Configuration Template

This folder contains a template configuration file for `target-mssql`, a Singer target for writing data to Microsoft SQL Server databases.

## Configuration File: config.json

### Required Configuration Options

#### Connection Method 1: Using SQLAlchemy URL

- **`sqlalchemy_url`** (string, optional if using individual connection params)
  - Full SQLAlchemy connection string for Microsoft SQL Server
  - Format: `mssql+pyodbc://username:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes`
  - If provided, this takes precedence over individual connection parameters
  - Note: Special characters in password should be URL-encoded

#### Connection Method 2: Using Individual Connection Parameters

If `sqlalchemy_url` is not provided, you must specify the following individual connection parameters:

- **`user`** (string, required if `sqlalchemy_url` not provided)
  - Database username for authentication
  - Example: `"sa"` or `"myuser"`

- **`password`** (string, required if `sqlalchemy_url` not provided)
  - Database password for authentication
  - Example: `"P@55w0rd"` or `"mypassword"`
  - Note: Special characters should be properly escaped

- **`host`** (string, required if `sqlalchemy_url` not provided)
  - Database server hostname or IP address
  - Example: `"localhost"` or `"192.168.1.100"` or `"sqlserver.example.com"`

- **`port`** (string, required if `sqlalchemy_url` not provided)
  - Database server port number
  - Default: `"1433"` (standard SQL Server port)
  - Example: `"1433"` or `"1434"`

- **`database`** (string, required if `sqlalchemy_url` not provided)
  - Target database name
  - Example: `"master"` or `"mydatabase"`

### Optional Configuration Options

- **`default_target_schema`** (string, optional)
  - Default schema name for creating tables when schema cannot be inferred from stream name
  - Default: `"dbo"` (if not specified and schema cannot be inferred)
  - Example: `"dbo"` or `"staging"` or `"analytics"`
  - If stream name contains a schema part (e.g., `schema-table`), that will be used instead
  - If stream name is `"public"`, it will be converted to `"dbo"`

- **`truncate`** (boolean, optional)
  - Global flag to truncate (drop and recreate) tables before loading data
  - Default: `false`
  - When `true`, all tables will be dropped and recreated before data is loaded
  - Can be overridden per-stream using `target-tables-config.json` (see below)
  - Example: `true` or `false`

- **`input_path`** (string, optional)
  - Path to directory containing `target-tables-config.json` for per-stream configuration
  - Default: Not set (per-stream config will not be loaded)
  - Example: `"./etl-output"` or `"/path/to/config/directory"`
  - If specified, the target will look for `target-tables-config.json` in this directory
  - Used for advanced per-stream configuration (truncate, replication_method, etc.)

## Per-Stream Configuration

If you set `input_path`, you can create a `target-tables-config.json` file in that directory to configure individual streams/tables:

```json
{
  "streams": {
    "stream_name_1": {
      "truncate": true,
      "replication_method": "truncate"
    },
    "stream_name_2": {
      "truncate": false,
      "replication_method": "merge"
    }
  }
}
```

### Per-Stream Options

- **`truncate`** (boolean, optional)
  - Whether to truncate this specific table before loading
  - Overrides global `truncate` setting

- **`replication_method`** (string, optional)
  - Replication method for this stream
  - Options: `"truncate"` (truncate table before load) or `"merge"` (upsert/merge data)
  - If set to `"truncate"`, the table will be dropped and recreated

## Configuration Examples

### Example 1: Using SQLAlchemy URL

```json
{
  "sqlalchemy_url": "mssql+pyodbc://sa:P%4055w0rd@localhost:1433/mydb?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
  "default_target_schema": "dbo"
}
```

### Example 2: Using Individual Parameters

```json
{
  "user": "sa",
  "password": "P@55w0rd",
  "host": "localhost",
  "port": "1433",
  "database": "mydatabase",
  "default_target_schema": "staging",
  "truncate": false
}
```

### Example 3: With Per-Stream Configuration

```json
{
  "user": "sa",
  "password": "P@55w0rd",
  "host": "localhost",
  "port": "1433",
  "database": "mydatabase",
  "default_target_schema": "dbo",
  "input_path": "./etl-output"
}
```

With `etl-output/target-tables-config.json`:
```json
{
  "streams": {
    "users": {
      "truncate": true
    },
    "orders": {
      "replication_method": "merge"
    }
  }
}
```

## Connection String Details

The target uses the following connection parameters by default:
- **Driver**: ODBC Driver 17 for SQL Server
- **Encryption**: Enabled (`Encrypt=yes`)
- **Trust Server Certificate**: Enabled (`TrustServerCertificate=yes`) - useful for development/testing
- **MARS Connection**: Enabled (`MARS_Connection=Yes`)
- **Connection Retry**: 3 attempts with 15-second intervals

## Usage

1. Copy `config.json` to your project directory
2. Update the connection parameters with your database credentials
3. Run the target:
   ```bash
   tap-your-source | target-mssql --config config.json > state.json
   ```

## Environment Variables

You can also use environment variables by setting `--config=ENV`. The target will automatically import environment variables from a `.env` file in the working directory.

Example `.env` file:
```
TARGET_MSSQL_USER=sa
TARGET_MSSQL_PASSWORD=P@55w0rd
TARGET_MSSQL_HOST=localhost
TARGET_MSSQL_PORT=1433
TARGET_MSSQL_DATABASE=mydatabase
```

