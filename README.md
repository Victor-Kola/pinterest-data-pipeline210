# Pinterest Data Pipeline

## Description
Pinterest processes billions of data points daily to enhance user value. This project aims to create a similar data pipeline system utilizing AWS cloud services. The current setup includes configuring an Amazon EC2 instance as an Apache Kafka client machine, creating Kafka topics through the MSK Management Console, and not using the AWS CLI.

### Key Learnings

While producing this Pinterest data pipeline, I have learnt the following insights by having hands-on experience with various AWS services and data engineering principles such as the data engineering lifecyyle - Generation, storage and ingestion.

- **Amazon EC2**: Set up and configured an EC2 instance in my local terminal to serve as an Apache Kafka client machine, including security aspects and connection setup.
- **Apache Kafka**: Learnt how to create and manage Kafka topics. Also learnt how to create Kafka consumers to see if my messages were being accurately consumed.
- **AWS Managed Streaming for Kafka (MSK)**: Gained an understanding of how to utilise MSK for scalable and managed Kafka deployments.
- **S3 Integration**: Acquired skills in integrating Kafka with S3 for data storage, using S3 connectors for data persistence.
- **IAM Roles**: Understood the importance of IAM roles for secure access management to AWS services.
- **MSK Connect**: Developed competency in setting up custom plugins in MSK Connect for extending functionality.
- **Configuration Management**: Learnt best practices for configuring Kafka clients and connectors to interact with AWS services.
- **Databricks**: Learnt and applied how to mount an s3 bucket onto Databricks. Also how to perform data cleaning and computations using Spark on Databricks. Part of these computations included joining dataframes together.
- **AWS Kinesis**: Learnt how to send streaming data to Kinesis and read this data into Databricks. Also how to format the streaming data so that it can be cleaned using Spark. An example of ETL as we loaded these streams into Delta Tables aswell. Finally how to read data from Kinesis Data Streams in Databricks.
- **Airflow**: Learnt how to define a DAG that runs a Databricks Notebook, how to create a MWAA to Databricks connection.  Then how to configure an Airflow UI that configures how tasks will run.


This project has been a practical platform for applying the above concepts in a cloud environment, simulating real-world data engineering tasks and challenges.


## Installation & Usage

### Setting Up the EC2 Instance
1. Log into the AWS Management Console.
2. Navigate to the EC2 Dashboard and launch a new instance.
3. Configure the instance with the required specifications.

### Kafka Client Setup on EC2
1. Connect to your EC2 instance using SSH.
2. Download and install Apache Kafka on the instance.
3. Configure Kafka to communicate with the MSK cluster.

### Configuring MSK Connect with a Custom Plugin
1. Identify your AWS S3 bucket that you previously created, which will be used for storing Kafka data.
2. Download and install the `Confluent.io Amazon S3 Connector` to your EC2 instance.
3. Create a custom plugin in the MSK Connect console with the name.

### Setting Up a Connector for Kafka to Amazon S3 Integration
1. Create a connector within MSK Connect with the name.
2. Configure the connector to use the correct S3 bucket for data storage.
3. Ensure the topics regex in the connector configuration matches the correct structure to route data from Kafka topics to the S3 bucket.
4. Select the IAM role with the correct format for the connector to authenticate to the MSK cluster.

### Sending Data to the API
- Successfully send data to the API endpoint by using the invoke URL.

### Setting up Databricks
- Mount the S3 Bucket to Databricks
- Read through the files to create data tables.
- After you have created these data tables, you will want to clean the tables so that all the data is uniform. This is part of the ELT process (Extract, Load, Transform).

### Setting up Streaming
- Create an airflow DAG that triggers a Databricks notebook to run on a schedule.
- Write this DAG script in Python and upload it to the appropriate bucket.
- Schedule the DAG  to run daily.
- Navigate to the MWAA environemnt and manually trigger the DAG.
- Confirm that the Databricks Notebook is executed as expected and monitor for successful completion.
- Create three data streams using Kinesis Data Streams.
- Configure the REST API to invoke Kinesis actions such as listing streams, creating streams and adding records to streams. 
- Activate the Python script to send data to the Kinesis stream, posting data to each stream.
- Use the Databricks notebook to process and clean the streaming data.
- Write the cleaned data to each of the corresponding Delta tables.


## File structure of the project 
1. user_posting_emulation_stream.py - Python file for streaming to AWS Kinesis.
2. user_posting_emulation_batch.py - Python file for batch uploading to AWS.
3. Pinterest Streaming Data.ipynb - Databricks notebooks for Pinterest streaming data processing. 
4. Pinterest Batch Data Pipeline.ipynb - Databricks notebook for Pinterest batch data processing.
5. 126dc60b95b3_dag.py - File required for databricks-airflow setup. 







