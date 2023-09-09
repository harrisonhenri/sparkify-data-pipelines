#!/bin/bash
/opt/airflow/start-services.sh
/opt/airflow/start.sh
airflow scheduler

# Add user
airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin

# Connections
airflow connections get aws_credentials -o json
airflow connections get redshift -o json
airflow variables set s3_bucket sean-murdock
airflow variables set s3_prefix sparkify-data-pipeline