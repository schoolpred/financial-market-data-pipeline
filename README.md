# financial-market-data-pipeline
Build a streaming data pipeline using Finnhub's real-time financial market data API. The architecture of this project primarily comprises five layers- Data Ingestion, Message broker, Stream processing, Serving database, and Visualization. The end result is a dashboard that displays data in a graphical format for deep analysis. 

The pipeline includes several components, including a producer that fetches data from Finnhub's API and sends it to a Kafka topic, a Kafka cluster for storing and processing the data. For stream processing, Apache Spark will be used. Next, Cassandra is used for storing the pipeline's financial market data that is streamed in real-time. Grafana is used to create the final dashboard that displays real-time charts and graphs based on the data in the database, allowing users to monitor the market data in real time and identify trends and patterns.
