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
- `scripts/` contains runnable modules for reconciliation and (future)
  repair tasks.

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
configuration enables logging output. Supported levels are `low`,
`medium` and `high`, where `high` produces the most verbose output.

This repository is intentionally minimal and focuses only on the core
logic for comparing tables. Many features are missing, including retry
logic, logging and tests.

