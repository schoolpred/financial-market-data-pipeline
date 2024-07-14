#!/bin/bash

# Define the paths
GCS_CONNECTOR_JAR_PATH="gcs-connector-hadoop2-2.2.2-shaded.jar"
SERVICE_ACCOUNT_JSON_PATH="deproject-427912-aeca8c315271.json"
SPARK_SCRIPT_PATH="./spark_transform/transform_json.py"
spark-submit \
  --jars $GCS_CONNECTOR_JAR_PATH \
  --conf "spark.hadoop.google.cloud.auth.service.account.enable=true" \
  --conf "spark.hadoop.google.cloud.auth.service.account.json.keyfile=$SERVICE_ACCOUNT_JSON_PATH" \
  --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
  --conf "spark.executor.instances=4" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  $SPARK_SCRIPT_PATH