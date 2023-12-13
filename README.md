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
- **Configuration Management**: Learned best practices for configuring Kafka clients and connectors to interact with AWS services.
- **Databricks**: Learned and applied how to mount an s3 bucket onto Databricks.
This project has been a practical platform for applying the above concepts in a cloud environment, simulating real-world data engineering tasks and challenges.


## Installation
*Note: The following instructions assume that you have an AWS account and the necessary permissions to create and manage EC2 instances and MSK clusters.*

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
- Successfully send data to the API endpoint by using the invoke URL 


### Prerequisites
- AWS Account
- Access to AWS Management Console
- Basic knowledge of Amazon EC2, MSK, and Apache Kafka

### Setting Up the EC2 Instance
1. Log into the AWS Management Console.
2. Navigate to the EC2 Dashboard and launch a new instance.
3. Configure the instance with the required specifications.

### Kafka Client Setup on EC2
1. Connect to your EC2 instance using SSH.
2. Download and install Apache Kafka on the instance.
3. Configure Kafka to communicate with the MSK cluster.

## Usage
After setting up the EC2 instance and Kafka client, you can create Kafka topics through the MSK Management Console.

Once MSK Connect is configured with the custom plugin and S3 connector, data produced to kafka topics will be automatically stored in the designated S3 Bucket.

*Further usage instructions will be provided as the project develops.*

## Configuration
*Details on Kafka client configuration and any other necessary setup will be added as the project progresses.*

## Features
*The specific features of the data pipeline will be outlined as they are developed.*

## Contributing
*Guidelines for contributing to the project will be provided in the future.*

## Credits
*Credits will be given to collaborators, mentors, and any third-party resources utilized.*

## License
*The project will be under a specific license, which will be determined and included here.*

## Contact Information
*Contact details for questions or discussions about the project will be added.*

## Project Status
*Ongoing - This project is currently under development.*

## Troubleshooting and FAQ
*This section will include common issues and their solutions as they are encountered.*
