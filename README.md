# spotify-end-to-end-data-engineering-project
ETL pipeline using AWS: Extracts data from Spotify API with AWS Lambda, stores raw/processed data in S3, infers schema with Glue Crawler and catalogs it in AWS Glue. Analyze structured data using Athena and automate via CloudWatch triggers.
## Introduction :
In this project, we build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline extracts data from the Spotify API, transforms it into the required format, and stores it in an AWS data store for analysis.


## Architecture :
!(Architecture Diagram) (https://github.com/darshilparmar/spotify-end-to-end-data-engineering-project/blob/main/Spotify_Data_Pipeline.jpg)

## Services Used

## S3 (Simple Storage Service):
### Amazon S3 is a scalable object storage service used to store and retrieve data efficiently. It is ideal for storing large media files, backups, and static website assets.

## AWS Lambda:
### AWS Lambda is a serverless compute service that executes code in response to events, such as changes in S3 or DynamoDB, without requiring server management.

## Amazon CloudWatch:
### CloudWatch is a monitoring service for AWS resources and applications. It tracks metrics, monitors logs, and enables alarms to ensure efficient system performance.

## AWS Glue Crawler:
### Glue Crawler is a managed service that automatically scans data sources, detects data formats, and infers schemas to populate the AWS Glue Data Catalog.

## AWS Glue Data Catalog:
### The Glue Data Catalog is a centralized metadata repository that simplifies data discovery and management. It integrates seamlessly with services like Amazon Athena for data querying.

## Amazon Athena:
### Athena is a serverless query service that uses standard SQL to analyze data stored in S3. It integrates with the Glue Data Catalog for efficient querying and analysis.
