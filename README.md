# Table Sync

This project provides a very small prototype for reconciling table data
between an Oracle database and a Microsoft SQL Server database. A YAML
configuration file defines the source and destination schemas and tables,
column mappings and a simple partitioning scheme. The included Python scripts
load this configuration, connect to both databases, fetch rows for each
partition and compare them column by column. Any discrepancies are
written to a table on the destination SQL Server, and can be automatically
fixed.

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

## Dependencies

This project requires the following Python libraries:

- `python-dateutil`
- `tqdm`
- `pandas`
- `pqdm`
- `psutil`

You will also need to install the appropriate database drivers for your
environment (e.g., `cx_Oracle`, `pyodbc`).

## Usage

1. **Configure:** Create a copy of `config/config.yaml` and adjust it for your
   environment. All values under the `env` keys reference environment
   variables that must be defined.
2. **Install Dependencies:** Install the required libraries from `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
3. **Run Reconciliation:** Run the `scripts/reconcile_runner.py` module:
   ```bash
   python scripts/reconcile_runner.py
   ```
   This will compare the data between the source and destination tables and
   write any discrepancies to the output table specified in the config file.
   By default, it will then automatically apply the fixes.

### Command-line Options

The `reconcile_runner.py` script accepts the following command-line options:

- `--config`: Path to the configuration YAML file to use. Defaults to
  `config/config.yaml`.
- `--debug`: Set the debug level (`low`, `medium`, or `high`).
- `--limit`: Maximum number of rows to fetch per table.
- `--output-mismatches`: Print discrepancy records to stdout.
- `--record`: Process only the record with this primary key value.

### Automatic Fixing

After the reconciliation process is complete, the `fix_mismatches.py` script
is automatically run to apply the necessary fixes to the destination table.
You can control this behavior using the `updates` section in the
`config.yaml` file.

To perform a dry run without applying any changes, you can run the script
manually:

```bash
python scripts/fix_mismatches.py
```

To apply the fixes, use the `--apply` flag:

```bash
python scripts/fix_mismatches.py --apply
```

## Features

- **Partitioning:** Supports partitioning to break down large tables into
  smaller, more manageable chunks.
- **Row Hashing:** Uses row hashing to quickly identify and skip identical
  rows, improving performance.
- **Parallel Processing:** Leverages parallel processing to speed up the
  comparison of partitions.
- **Automatic Fixing:** Automatically applies fixes for mismatched rows,
  updating only the incorrect columns.
- **Dry Run Mode:** Allows you to preview the SQL statements that will be
  executed to fix mismatches without actually applying them.

