# Table Sync

This project provides a very small prototype for reconciling table data
between an Oracle database and a Microsoft SQL Server database. A YAML
configuration file defines the source and destination schemas and tables,
column mappings and a simple partitioning scheme. The included Python scripts
load this configuration, connect to both databases, fetch rows for each
partition and compare them column by column. Any discrepancies are
written to a table on the destination SQL Server.

## Structure

- `config/` contains the `config.yaml` example used for the run.
- `connectors/` holds helpers for creating database connections.
- `logic/` implements utility functions for loading configuration,
  comparing rows and writing reports.
- `runners/` provides lower level functions invoked by the command line
  scripts.
- `scripts/` contains runnable modules for reconciliation and applying
  fixes for mismatched rows.
- `tests/` holds unit tests covering the core logic.

## Usage

1. Create a copy of `config/config.yaml` and adjust it for your
   environment. All values under the `env` keys reference environment
   variables that must be defined.
2. Install required libraries (for example `cx_Oracle`, `pyodbc` and
   `python-dotenv`).
3. Run the `scripts/reconcile_runner.py` module:
   ```bash
   python scripts/reconcile_runner.py
   ```
   Discrepancies will be written to the table specified in the config
   file.

Passing the `--debug` flag or setting a debug level in the YAML
configuration enables logging output written to `debug.log` in the project
root. Supported levels are `low`, `medium` and `high`, where `high` produces
the most verbose output.

To limit the number of rows fetched during testing, pass the `--limit`
option to `reconcile_runner.py`:

```bash
python scripts/reconcile_runner.py --limit 50000
```
This restricts the amount of data retrieved from each table by applying a
`FETCH FIRST`/`OFFSET` clause in the SQL queries, which speeds up test runs.

Use the `--output-mismatches` flag to print discrepancy records to stdout instead
of solely writing them to the SQL Server table. This is useful when testing
connectivity issues:

```bash
python scripts/reconcile_runner.py --output-mismatches
```

When debugging specific issues you can process a single row by supplying its
primary key value using the `--record` option:

```bash
python scripts/reconcile_runner.py --record 12345
```

Row comparison can optionally use a row hash to skip columns when the
source and destination rows are identical. Parallel comparison across
partitions is also supported when enabled in the YAML config.
When weekly partitions are defined for a month, all weeks are processed
concurrently. Queries for the next week wait until the previous week's
fetch has completed so database load is staggered.

After reconciliation, mismatches can be applied back to the destination
table using:

```bash
python scripts/fix_mismatches.py --apply
```
This script updates only the differing columns and accepts a `--no-dry-run`
flag when you want to preview the SQL statements.

This repository remains intentionally small but now includes simple
logging via `utils.logger`, progress bars driven by `pqdm` and a suite of unit tests.
Retry logic and advanced error handling are still out of scope to keep
the example focused on reconciliation.

