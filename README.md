# Financial Market data pipeline

Build a streaming data pipeline using Finnhub's real-time financial market data API. The workflow involves retrieving data from the Finnhub API, transforming it, streaming it through a Kafka server, saving files locally, processing the data with PySpark, storing it in Google Cloud Storage, and performing data modeling in BigQuery. 

![](https://github.com/schoolpred/financial-market-data-pipeline/blob/main/Project_Flowchart.gif)

## Project Workflow

1. **Data Retrieval**
    - Fetch data from the Finnhub API.

2. **Data Transformation**
    - Perform initial transformations on the fetched data.

3. **Data Streaming**
    - Send the transformed data to a Kafka server.

4. **Data Storage**
    - Receive and save the streamed data files locally.

5. **Data Processing**
    - Use PySpark to further transform the raw data.

6. **Cloud Storage**
    - Load the processed data to Google Cloud Storage.

7. **Data Modeling**
    - Perform data modeling and analysis in BigQuery.

## Repository Structure
```sh
├── config
│ ├── config.yaml
├── data_modeling
│ ├── bigquery_modeling.sql
├── etl
│ ├── __init__.py
│ ├── extract.py
│ ├── transform.py
├── kafka
│ ├── setup-kafka.txt
├── pipelines
│ ├── consumer.py
│ ├── producer.py
├── spark_transform
│ ├── run.sh
│ ├── transform_json.py
├── README.md
└── .gitignore
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- Java 8+
- Apache Kafka
- Google Cloud SDK
- PySpark
- Finnhub API Key

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/schoolpred/financial-market-data-pipeline.git
    cd financial-market-data-pipeline
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

3. Set up Kafka server and create necessary topics.

4. Configure Google Cloud Storage and BigQuery access.

### Running the Project

1. **Data Retrieval, Transformation and Saving Files Locally**
    ```sh
    python3 pipelines/consumer.py
    python3 pipelines/producer.py
    ```

2. **Data Processing with PySpark and Uploading processed Data to Google Cloud Storage**
    ```sh
    . spark_transform/run.sh
    ```

6. **Data Modeling in BigQuery**
    - Execute the SQL script `data_modeling/bigquery_modeling.sql` in BigQuery.

## Usage

- Customize the scripts as needed to fit your data and requirements.
- Ensure that the necessary configurations for API keys and cloud services are set up properly.

## Contact

For any questions or feedback, please contact me at:
- Name: Truong Le
- Email: [lequangtruong1108@gmail.com]
- Linkedln: [www.linkedin.com/in/truong-le-quang-data-analyst]

---

Thank you for checking out my project! I hope it demonstrates my skills and capabilities as a Data Engineer.
