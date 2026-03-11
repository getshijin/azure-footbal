# Azure Football Data Pipeline

This repository contains a small Apache Airflow ETL pipeline that collects football stadium data from Wikipedia, transforms it with pandas, and writes the result to Azure Data Lake Storage (ABFS).

## What this project does

- Extracts stadium capacity table data from:  
  `https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity`
- Cleans and normalizes fields such as capacity, stadium names, and image URLs.
- Loads the processed dataset as a CSV file into an Azure Data Lake container.

## Project structure

- `dags/wikipedia_flow.py` – Airflow DAG definition (`wikipedia_flow`) with three tasks:
  1. `extract_data_task_id`
  2. `transform_task_id`
  3. `write_task_id`
- `pipelines/wikipedia_pipeline.py` – ETL logic for extract/transform/load.
- `script/entrypoint.sh` – Airflow startup script (installs requirements, initializes DB/user, starts webserver).
- `test.py` – quick local script to validate the target Wikipedia table can be fetched and parsed.
- `requirements.txt` – Python dependencies.

## Prerequisites

- Python 3.12 (per `Pipfile`)
- Apache Airflow 2.10.5
- Network access to Wikipedia
- Azure Data Lake Storage Gen2 account/container

## Configuration

The pipeline expects an Azure storage account key imported as:

- `pipelines.secret.aws_access_key`

Create a `pipelines/secret.py` file (or equivalent secret handling) that defines:

```python
aws_access_key = "<your-azure-storage-account-key>"
```

> Note: Keep secrets out of version control.

## Running the pipeline

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Start Airflow webserver (using provided entrypoint)

```bash
bash script/entrypoint.sh
```

### 3) Trigger the DAG

- Open Airflow UI.
- Enable and trigger `wikipedia_flow`.
- Tasks run in order: extract → transform → write.

## Output

A CSV file is written to:

`abfs://footballdata@footballdataenginee.dfs.core.windows.net/data/`

with filename pattern similar to:

`stadium_cleaned_<date>_<time>.csv`

## Notes

- Geocoding logic exists in `pipelines/wikipedia_pipeline.py` but is currently commented out.
- If Wikipedia table structure changes, extraction selectors may need updates.
