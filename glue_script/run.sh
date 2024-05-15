#!/bin/bash

# Define your application's JAR file
APP_JAR="path/to/your/application.jar"

# Define the main class of your application, if necessary
MAIN_CLASS="com.example.MainClass"

# Define any other necessary configuration or arguments for your application
OTHER_ARGS=""

# Use spark-submit with desired configurations
spark-submit \
    --deploy-mode client \
    --conf spark.driver.memory=15g \
    transform_data.py
