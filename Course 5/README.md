# ELT Data Pipeline for P4

This README outlines the ELT data pipeline developed for the final project (p4), detailing the primary operators, their roles, and the associated DAG calls.

## Files

All reference project files are located in their default directories, including:

- /home/workspace/set_connections_and_variables.sh
- /home/workspace/airflow/plugins/final_project_operators/data_quality.py
- /home/workspace/airflow/plugins/final_project_operators/load_fact.py
- /home/workspace/airflow/plugins/custom_operators/load_dimensions.py
- /home/workspace/airflow/plugins/final_project_operators/stage_redshift.py
- /home/workspace/airflow/dags/udacity/common/final_project_sql_statements.py
- /home/workspace/airflow/dags/cd0031-automate-data-pipelines/project/starter/final_project.py

## Operators

### StageToRedshiftOperator

This operator loads JSON files from Amazon S3 into Amazon Redshift, dynamically generating and executing a SQL COPY statement based on input parameters.

### LoadFactOperator

Responsible for loading data into a Redshift fact table, this operator executes an SQL INSERT command to add data to the specified table.

### LoadDimensionOperator

This operator loads data into a Redshift dimension table, supporting both append-only and delete-load functionalities.

### DataQualityOperator

This operator conducts dynamic quality control tests as defined in the DAG, helping to identify data quality issues, mismatches, and unwanted defaults.

## Custom SQL Queries

The SqlQueries class contains SQL queries for inserting data into fact and dimension tables. It can also create tables once within the DAG for testing and standalone functionality, while production-level table creation should be handled by external operators.

## DAG Structure

The structure of the DAG is defined in the final_project Airflow DAG file.

## Additional Functionality

The pipeline includes features like logging, operational mode switching, and data quality checks, with 15-20 basic tests for DataQuality checks. A dimension subDAG was initially implemented and tested but later commented out to expedite testing of new features.

# Extra Features Guidelines

## Timed Template in Stage Operator

To enable timed template functionality in the StageToRedshiftOperator:

1. Set the `s3_key` parameter in the DAG definition to include a time template string, e.g., `s3_key='log_data/{{ execution_date.strftime("%Y-%m-%d") }}'`.
2. This replaces `{{ execution_date.strftime("%Y-%m-%d") }}` with the execution date in `YYYY-MM-DD` format. By default, this is disabled for resource efficiency and cost savings.

## Switching between Append-Only and Delete-Load Dim. functionality

To toggle between append-only and delete-load functionality in the DAG:

1. Set the `append_mode` parameter of the LoadDimensionOperator to `True` for append-only or `False` for delete-load mode.
2. The LoadDimensionOperator will perform the corresponding data loading operation based on the `append_mode` value.

## Switch Option in Load Dimension Operator

The LoadDimensionOperator supports both modes. To switch:

1. Set the `append_mode` parameter to `True` for append-only or `False` for delete-load mode (e.g., `append_mode=True`).
2. If `append_mode` is `True`, it performs an `INSERT INTO` operation. If `False`, it executes a `TRUNCATE TABLE` command before loading new data.
   By default, this is disabled for resource efficiency and cost savings.

## Hooks and Dynamic Parameters

Operators use hooks to connect with Redshift and execute SQL statements, dynamically passing parameters to generate SQL queries.

1. The `redshift_conn_id` parameter is set to the Airflow connection ID for Redshift.
2. SQL statements utilize f-strings to inject parameters dynamically.

## DB Infrastructure: Amazon Redshift

We have tested both the hosted and serverless versions of AWS Redshift. For classic Redshift, use the provided U_IaC_v1.1.ipynb to manage AWS resources and the Redshift cluster without needing the AWS console.

## Running the Pipeline

To execute the data pipeline, ensure Apache Airflow is set up and configured. Import the DAG file into the Airflow environment and trigger it manually or on a schedule. The DAG, including a one-time 7x table creation, runs in approximately 90 seconds on the "A" paths (without local copies) for the subset below:

- s3_bucket='udaicty-project-final'
- s3_key='song_data/A/'

## Conclusion

This ELT data pipeline offers a scalable and automated method for loading data from various sources into Amazon Redshift. By utilizing Airflow's robust features and custom operators, the pipeline facilitates efficient data processing and transformation.
