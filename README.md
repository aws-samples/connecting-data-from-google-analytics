#  Guidance for Connecting Data from Google Analytics to AWS Clean Rooms

This guidance provides a reference architecture for the ingestion of Google Analytics v4 (GA4) data from Google Cloud Big Query into AWS for data analytics and collaboration.

## Overview

Solutions guidance for ingesting Google Analytics 4 data into AWS for marketing analytics and activating marketing channels with customized audience profiles. 

This sample solution will help organizations securely ingest first-party data from GA4 and combine it with third-party data in AWS. As a result, marketers can further use this information to activate marketing channels with customized segments.

## Solution Architecture

![Alt text](img/arch-1.1.png?raw=true "Architecture")

The architecture uses a Step Function to orchestrate the AWS Glue job and data crawling. The Step Function is executed and extracts the previous day's GA4 data from BigQuery.

1.	Amazon EventBridge scheduled rule executes and starts the AWS Step Function workflow.
2.	Google Cloud Big Query access credentials are securely stored in Secrets Manager and encrypted with AWS Key Management Service (KMS).
3.	AWS Glue job will ingest data using the AWS Marketplace Google BigQuery Connector for AWS Glue. The Connector simplifies the process of connecting AWS Glue jobs to extract data from BigQuery. This AWS Glue job will encrypt, normalize, and hash the data. 
4.	The output of the AWS Glue job is written to the target Amazon Simple Storage Service (Amazon S3) bucket:prefix location in parquet format. After an AWS Clean Rooms collaboration, custom audience data such as emails, phone numbers, or mobile advertiser IDs are hashed, encrypted, and stored in designated prefixes. The output file setting is partitioned by date and encrypted with AWS KMS.
5.	AWS Glue Crawler job is triggered to "refresh" the table definition and its associated meta-data in the AWS Glue Data Catalog.
6.	The Data Consumer queries the data output with Amazon Athena.

![Alt text](img/arch-2.png?raw=true "Step Function Workflow")

# Deploying the Solution

## Manual Setup: Exporting Data from Google Analytics 4 Properties to BigQuery

1. Follow the setup from [Exporting Data from Google Analytics 4 Properties to BigQuery](https://support.google.com/analytics/answer/9358801?hl=en)
2. Note the GCP Project ID and the BigQuery Dataset ID.

## Deploying the AWS Data Ingestion Solution

The project code uses the Python version of the AWS CDK ([Cloud Development Kit](https://aws.amazon.com/cdk/)). To execute the project code, please ensure that you have fulfilled the [AWS CDK Prerequisites for Python](https://docs.aws.amazon.com/cdk/latest/guide/work-with-cdk-python.html).

The project code requires that the AWS account is [bootstrapped](https://docs.aws.amazon.com/de_de/cdk/latest/guide/bootstrapping.html) to allow the deployment of the CDK stack.


### Update the CDK Context Parameters

Update the cdk.context.json with 

```
{
    "job_name": "bigquery-analytics",
    "data_bucket_name": "your-s3-bucket-name",
    "dataset_id": "Big Query Dataset ID",
    "parent_project": "GCP Project ID",
    "connection_name": "bigquery",
    "filter": "",
    "job_script": "job-analytics.py",
    "schedule_daily_hour": "3",
    "glue_database": "gcp_analytics",
    "glue_table": "ga4_events",
    "timedelta_days": "1"
}
```
#### Context Parameter Summary

1.	job_name – the name of the AWS Glue job
2.  data_bucket_name - bucket name for the data and Glue Job Script
3.	dataset_id – BigQuery Dataset ID for the Google Analytics 4 export
4.	parent_project – GCP Project ID
5.	connection_name – Glue Connection name
6. filter - not currently used however, it can be used for BigQuery query filtering
7. job_script - job-analytics.py - this included AWS Glue script pulls and flattens data from BigQuery
8. schedule_daily_hour - - default 3 AM - daily schedule hour of the job runs to get yesterday's analytics data
9. glue_database - Glue database name
10. glue_table - Glue table name
11. timedelta_days: Number of days back to pull events.   0 = today, 1 = yesterday, default 1


## CDK Deployment

```
# navigate to project directory
cd aws-glue-connector-ingestion-ga4-analytics

# install and activate a Python Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

# install dependant libraries
python -m pip install -r requirements.txt


# cdk bootstrap 

$ Upon successful completion of `cdk bootstrap` the project is ready to be deployed.

cdk deploy 
```
Alternative: Run the cdk_deploy.sh script for all the above steps

```
# navigate to project directory
cd aws-glue-connector-ingestion-ga4-analytics

# install cdk, setup the virtual environment, bootstrap, and deploy

./cdk_deploy.sh
```
## Create GCP Access Credentials
   1. Create and Download the service account credentials JSON file from Google Cloud. [Create credentials for a GCP service account](https://developers.google.com/workspace/guides/create-credentials#service-account)
   2. base64 encode the JSON access credentials. For Linux and Mac, you can use base64 <<service_account_json_file>> to output the file contents as a base64-encoded string

### Adding GCP Credentials in AWS Secrets Manager
   1. In AWS Secret Manager, paste the base64-encoded GCP credentials into the `bigquery_credentials` `credentials` Secret value with the base64 encode access credentials

![Alt text](img/secret_manager-1.png?raw=true "Secret Manager Secret")

![Alt text](img/secret_manager-2.png?raw=true "Secret Manager Secret")

![Alt text](img/secret_manager-3.png?raw=true "Secret Manager Secret")


## Manual Setup: Subscribe to the Google BigQuery Connector for AWS Glue

   Subscribing to the  [Google BigQuery Connector for AWS Glue](https://aws.amazon.com/marketplace/pp/prodview-sqnd4gn5fykx6?sr=0-1&ref_=beagle&applicationId=GlueStudio) in the AWS Marketplace

![Alt text](img/big-query-0.png?raw=true "Google BigQuery Connector for AWS Glue")

   1. Choose Continue to Subscribe.
   2. Review the terms and conditions, pricing, and other details.
   3. Choose Continue to Configuration.
   4. For Fulfillment option, choose the AWS Glue Version you are using (3.0).
   5. For Software Version, choose your software version

   ![Alt text](img/big-query-1.png?raw=true "Google BigQuery Connector for AWS Glue Setup")

   1. Choose to Continue to Launch
   2. Under Usage instructions, review the documentation, then choose to Activate the Glue connector from AWS Glue Studio.

   ![Alt text](img/big-query-2.png?raw=true "Google BigQuery Connector for AWS Glue Setup")


   1. You’re redirected to AWS Glue Studio to create a Connection.

  ![Alt text](img/big-query-3.png?raw=true "Google BigQuery Connector for AWS Glue Setup")

   2. For Name, enter a name for your connection (for example, bigquery).
   3.  For AWS Secret, choose bigquery_credentials.
   4.  Choose to Create a connection and activate the connector.
   5.  A message appears that the connection was successfully created, and the connection is now visible on the AWS Glue Studio console.

## Testing

1. Option 1: Wait 24 hours for the Amazon EventBridge schedule to execute the AWS Glue job to run
2. Option 2: Manually Execute the Step Function named `gcp-connector-glue`  
3. After the Step Function completes, go to Athena
   1. Select Data source: `AwsDataCatalog`
   2. Select the Database: `ga4_analytics` 
   3. Query `SELECT * FROM your ga4_events`


![Alt text](img/athena-query.png?raw=true "Sample Athena Query")


## Sample Queries

See a list of [Sample Queries](assets/scripts/athena_queries.sql).



[Views](assets/scripts/athena_views.sql)  can also flatten the query results for the complex json data types. The views may need to be modified based on the table and database names.




## Cleanup

When you are finished experimenting with this solution, clean up your resources by running the command:

```
cdk destroy 
```

This command will delete the resources deployed by the solution. The AWS Secret Manager secret containing the manually added GCP Secret and CloudWatch log groups are retained after the stack is deleted.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

## References

1. [Google BigQuery Connector for AWS Glue](https://aws.amazon.com/marketplace/pp/prodview-sqnd4gn5fykx6?sr=0-1&ref_=beagle&applicationId=GlueStudio)
2. [Exporting Data from Google Analytics 4 Properties to BigQuery](https://support.google.com/analytics/answer/9358801?hl=en)
3. [Migrating data from Google BigQuery to Amazon S3 using AWS Glue custom connectors](https://aws.amazon.com/blogs/big-data/migrating-data-from-google-bigquery-to-amazon-s3-using-aws-glue-custom-connectors/)
4. [Create credentials for a GCP service account](https://developers.google.com/workspace/guides/create-credentials#service-account)
