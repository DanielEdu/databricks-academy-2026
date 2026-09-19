# adls_landing

Ingests source tables from the ADLS landing layer (`abfss://landing@stecopocdeveastus001.dfs.core.windows.net/`)
into Unity Catalog Bronze using Auto Loader.

Each table goes through two tasks: a DDL task creates the table explicitly
before Auto Loader ever writes to it, then the ingestion task appends to it.

* `src/_table_schemas.py`: single source of truth for column names — the
  business columns per table (all `STRING`, raw as landed) and the
  standardized control columns (`_ingested_at` TIMESTAMP, `_source_file`
  STRING, `_rescued_data` STRING). Included via `%run` by both notebooks below.
* `src/create_bronze_tables.py`: parameterized notebook (`catalog`, `schema`,
  `table` widgets) that runs `CREATE TABLE IF NOT EXISTS` with that fixed
  schema — so table structure is explicit and versioned, not whatever Auto
  Loader happens to infer on first write. Tables are created append-only
  (`delta.appendOnly = true`): Auto Loader only ever appends, so
  `UPDATE`/`DELETE`/`MERGE` are rejected at the table level.
* `src/ingest_autoloader.py`: parameterized notebook (`catalog`, `schema`,
  `source`, `table`, `landing_path`, `checkpoint_path` widgets) that reads the
  same fixed all-`STRING` schema via Auto Loader (`cloudFiles`), with schema
  evolution disabled — any new/renamed source column lands in
  `_rescued_data` instead of silently altering the Bronze table.
* `resources/adls_landing.job.yml`: job definition with a `ddl_<table>` task
  followed by a `<table>` ingestion task (`depends_on` the DDL task), per
  source table, all reusing the two notebooks above.
* Auto Loader checkpoint and schema location live under the
  `abfss://checkpoints@stecopocdeveastus001.dfs.core.windows.net/` external location.

## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

## Using this project using the CLI

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks auth login --profile ecopoc-dev
    ```

2. Validate the bundle:
    ```
    $ databricks bundle validate --target dev --profile ecopoc-dev
    ```

3. Deploy the job (does not run it):
    ```
    $ databricks bundle deploy --target dev --profile ecopoc-dev
    ```

4. To add a new source table: add its column list to `TABLE_COLUMNS` in
   `src/_table_schemas.py`, then add a `ddl_<table>` task and a `<table>`
   ingestion task (`depends_on` the DDL task) to
   `resources/adls_landing.job.yml`, mirroring an existing pair.

5. The job runs on a daily schedule (`0 0 0 * * ?`, America/Lima, every day at
   12:00 AM). It is deployed but not triggered automatically on deploy.
