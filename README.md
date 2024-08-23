#  Snowflake Data Migration

This repository contains SQL scripts for migrating data from Amazon S3 to Snowflake tables using the COPY INTO command. The scripts are designed to facilitate the seamless transfer of data stored in Parquet format, with the necessary transformations to ensure data integrity and accuracy.

## Prerequisites
Before using these scripts, ensure you have:

- A Snowflake account with the necessary privileges.
- Access to the Amazon S3 bucket containing the data.
- The ARN of the AWS IAM role with the required permissions to access the S3 bucket.

## Key Steps in the Migration Process
1 File Format Creation

- Define the file format in Snowflake to read and interpret data from Parquet files.

2 Storage Integration Setup

- Establish a secure connection between Snowflake and Amazon S3 through a storage integration.
  
3 Stage Creation

- Create a stage in Snowflake to act as an intermediary storage location for data from S3.
  
4 Data Preview

- Verify the data by listing and querying the contents of the stage to ensure correct access and format.
  
5 Data Transformation

- Apply necessary transformations to the data, converting columns to the appropriate data types before loading.

6 Copying Data to Snowflake
  
- Execute the COPY INTO command to load the transformed data into the target Snowflake table.
