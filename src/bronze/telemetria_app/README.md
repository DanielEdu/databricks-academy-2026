# telemetria_app

Streaming ingestion of the `wizard.lending.eventos-app` topic (Azure Event Hubs,
Kafka-compatible endpoint) into Unity Catalog Bronze (`bronze_dev.telemetria_app.eventos_app`).

* `src/ingest_kafka_streaming.py`: single notebook that reads the Kafka topic,
  flattens the event contract into string columns, keeps the Kafka record
  metadata as a `_metadata` JSON column, and stamps `_ingested_at` with the
  Bronze landing timestamp.
* `resources/telemetria_app.job.yml`: serverless job definition with a
  job-level `continuous` trigger.
* Credentials (`eventhub-connection-string`, `eventhub-namespace`) are read
  from the `wizardbank` secret scope — nothing is hardcoded.

## Why `continuous` job + `trigger(availableNow=True)`, not `processingTime`

Serverless compute only supports bounded Structured Streaming triggers.
`Trigger.ProcessingTime` and `Trigger.Continuous` fail on serverless with
`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. The supported pattern for
always-on ingestion on serverless is: the query uses
`trigger(availableNow=True)` and exits after draining available offsets, and
the job's `continuous` trigger immediately restarts the task — the streaming
checkpoint guarantees no offset is reprocessed across restarts.

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

4. Start it (job is deployed paused-by-default under `mode: development`; unpause it to start the continuous run):
    ```
    $ databricks bundle run kafka_telemetria_app_to_bronze --target dev --profile ecopoc-dev
    ```

5. The job is `continuous` (always running, one active run at a time, auto-restart on completion/failure).
   To stop it, pause it from the Jobs UI or set `pause_status: PAUSED` and redeploy.
