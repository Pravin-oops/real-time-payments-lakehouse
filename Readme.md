# 🛠️ INITIAL CLOUD AND LOCAL SETUP

The pipeline is going to run mostly on cloud infrastructure, with some data generation occurring on a local machine. I have selected cloud vendors that offer free trial periods to facilitate practice and learning.

## 1. Confluent.io Kafka
I have chosen **kafka datagen** as one of the raw data source to be used in the projects.
### i. Account Creation and perks
Confluent.io provides **1 month of free usage and 400$ credit** limit for first time account creation.
**Link** : [Get started with Confluent.io](https://www.confluent.io/get-started/)
### ii. Cluster and API Key creation
* While selecting the cloud provider choose AWS basic (Its closer for me as I will be hosting bronze layer of our medallion architecture in AWS mumbai location)
* Create a cluster name **kafka_cluster** with basic settings.
* Make sure in the cluster settings use only one eCKUs
* Navigate to API Keys in the cluster menu.
* Click Create key and select Global Access
* CRITICAL: Copy the API Key and API Secret.
### iii. Topic Creation
* Inside the new cluster, navigate to Topics on the left-hand menu.
* Click Create Topic.
* Topic Name: credit_card_transactions
* Partitions: Set to 1 (sufficient for a local development stream).
* Save and create the topic.
### iv. Custom Data generation by kafka
* we can generate this data from our local machine itself but doing it by kafka we can use its several feature, which could be hassle if you want to replicate it in your local system
* Click on connector option in the left and search for datagen
* On the pop up select additional configuration and select the topic where you want to populate the data.
* on kafka credentials choose using an existing API key and enter the key values you generated in the previous step.
* on the configuration page select **Provide your own schema** and copy paste the **kafka_datagen_credit_card_schema.json** file content into the JSON coding area
* continue creating the connector with default settings. on the final stage you can see connector active.
* once connector is active you can see the values flowing into the topic we have created.



## 2. AWS Account
I have selected AWS as the cloud provider for hosting the bronze layer of our medallion architecture
### i. Account Creation and perks
AWS provides **6 months of free tier access** with a variety of services available for free within certain usage limits.
**Link** : [Create an AWS Account](https://aws.amazon.com/free/)
### ii. Bucket Creation
* After creating your AWS account, navigate to the S3 service.
* Click on "Create bucket".
* Bucket Name: `upi-payments-lakehouse`
* Region: Choose the region closest to you (e.g., Asia Pacific (Mumbai) - ap-south-1).
* Leave the other settings as default and create the bucket.
### iii. IAM User Creation & Access Key Generation
* Navigate to the IAM (Identity and Access Management) service.
* Click on "Users" and then "Create user".
* User Name: `lakehouse-user`
* Access Type: Select "Programmatic access" to generate an access key for API interactions.
* Click "Next: Permissions".
* Choose "Attach existing policies directly" and select the "AmazonS3FullAccess" policy to grant full access to S3.
* Click "Next: Tags" (optional) and then "Next: Review".
* Review the user details and click "Create user".
* On the final page, copy the Access Key ID and Secret Access Key. Store them securely, as you will need them to configure your data pipeline.



## 3. Databricks Account
I have selected Databricks as the cloud provider for hosting the silver and gold layers of our medallion architecture
### i. Account Creation and perks
Databricks provides **free trial access** with their free edition with a variety of features available for free without need of credit card in the account creation process.
**Link** : [Create a Databricks Account](https://databricks.com/try-databricks)
### ii. Notebook Creation
* Region: free edition workspaces are serverless and do not require a specific region selection.
* After creating your Databricks account, navigate to the Databricks workspace.
* Click on "Workspace" and then create a new notebook under your user folder.
* Notebook Name: `01_silver_ingestion_pipeline`
### iii. setting databricks token
* Because Serverless compute is a shared environment, it blocks raw credential configurations (e.g., spark.conf.set()). Authentication must be handled via Databricks Secret Scopes or inline URI injection.
* In free edition, you can only create a secret scope using the Databricks CLI. So we need to create databricks token first to authenticate the CLI.
* Click on the user icon in the top right corner and select "User Settings".
* navigate to users -> Developers -> Manage tokens -> Generate New Token.
* token name: `VS_code_CLI`
* token lifetime: 90 days
* Click "Generate" and copy the generated token. Store it securely, as you will need it to configure the Databricks CLI.

#### iv. Secret Scope Creation and Credential Injection
* Install the Databricks CLI on your local machine using pip: pip install databricks-cli
* Configure the Databricks CLI with your workspace URL and the generated token:
  ```
  databricks configure --token
  ```
  - Databricks Host: `https://<your-workspace-url>`
  - Token: `<your-generated-token>`
* Create a secret scope named `lakehouse-secrets` using the Databricks CLI:
  ```
  databricks secrets create-scope --scope lakehouse_secrets
  ```
  *  specific issue I went through is since I run light weight linux distro in docker container, it doesn't have a default text editor to write the secret value, so I used the following command to create the secret with inline value injection:
  ```
  databricks secrets put --scope lakehouse_secrets --key aws_access_key --string-value <your-aws-access-key-id>
  databricks secrets put --scope lakehouse_secrets --key aws_secret_key --string-value <your-aws-secret-access-key>

* After running the above commands, your AWS credentials will be securely stored in the Databricks Secret Scope and can be accessed in your notebooks using the following syntax:
    ```
    dbutils.secrets.get(scope="lakehouse_secrets", key="aws_access_key")
    dbutils.secrets.get(scope="lakehouse_secrets", key="aws_secret_key")
    ```
## 4. Local Environment Setup
### i. Python Environment
* Docker devcontainer setup with Python 3.10 and necessary libraries.
* Create a `requirements.txt` file with the following content:
```
boto3==1.34.51
python-dotenv==1.0.1
pyspark==3.5.0
jupyter==1.0.0
```
* Install the dependencies using dockerfile setup
### ii. Environment Variables
* Create a `.env` file in the root of your project directory with the following content:
```
# AWS Credentials
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
S3_BUCKET_NAME=YOUR_S3_BUCKET_NAME

# Confluent Kafka Credentials
KAFKA_BOOTSTRAP_SERVER=YOUR_KAFKA_BOOTSTRAP_SERVER
KAFKA_API_KEY=YOUR_KAFKA_API_KEY
KAFKA_API_SECRET=YOUR_KAFKA_API_SECRET

# databricks_access_token
DATABRICKS_ACCESS_TOKEN=YOUR_DATABRICKS_ACCESS_TOKEN
DATABRICKS_HOST=YOUR_DATABRICKS_HOST_URL
```
* Replace the placeholder values with your actual credentials and host URL.
* to check if credentials are saved properly check in terminal
```
databricks secrets list --scope lakehouse_secrets
```
