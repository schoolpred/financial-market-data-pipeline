#!/bin/bash

spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.375 \
    transform_json.py