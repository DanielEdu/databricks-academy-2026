# adls_landing

Ingests source tables from the ADLS landing layer (`abfss://landing@stecopocdeveastus001.dfs.core.windows.net/`)
into Unity Catalog Bronze using Auto Loader.

* `src/ingest_autoloader.py`: single parameterized notebook used by every task
  (`catalog`, `schema`, `source`, `table`, `landing_path`, `checkpoint_path` widgets).
* `resources/adls_landing.job.yml`: job definition with one task per source table,
  all reusing the notebook above.
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

4. To add a new source table, add another task to
   `resources/adls_landing.job.yml` pointing at the same notebook with a
   different `table` base parameter.

5. The job runs on a daily schedule (`0 0 0 * * ?`, America/Lima, every day at
   12:00 AM). It is deployed but not triggered automatically on deploy.
