# 🛠️ INITIAL CLOUD ACCOUNT SETUP

The pipeline is going to run mostly on cloud infrastructure, with some data generation occurring on a local machine. I have selected cloud vendors that offer free trial periods to facilitate practice and learning.

## 1. Confluent.io Kafka
I have chosen **kafka datagen** as one of the raw data source to be used in the projects.

### i. Account Creation and perks
Confluent.io provides **1 month of free usage and 400$ credit** limit for first time account creation.

**Link** : [Get started with Confluent.io](https://www.confluent.io/get-started/)

### ii. Cluster and API Key creation
* While selecting the cloud provider choose AWS basic (Its closer for me as I will be hosting bronze layer of our medallion architecture in AWS mumbai location)

* Create a cluster name **kafka_cluster** with basic settings the final screen looks like below
![alt text]({E6AF4D9D-4125-48A8-B3B6-84A01874D28D}.png)

* Make sure in the cluster settings use only one eCKUs
![alt text](image.png)

* Navigate to API Keys in the cluster menu.
* Click Create key and select Global Access
* CRITICAL: Copy the API Key and API Secret.
![alt text](image-1.png)

### iii. Topic Creation
* Inside the new cluster, navigate to Topics on the left-hand menu.
* Click Create Topic.
* Topic Name: credit_card_transactions
* Partitions: Set to 1 (sufficient for a local development stream).
* Save and create the topic.

![alt text]({96A533B0-98FA-40CE-A762-7FCB4D1AF913}.png)

### iv. Custom Data generation by kafka
* we can generate this data from our local machine itself but doing it by kafka we can use its several feature, which could be hassle if you want to replicate it in your local system
* Click on connector option in the left and search for datagen
![alt text](image-3.png)
* On the pop up select additional configuration and select the topic where you want to populate the data.
* on kafka credentials choose using an existing API key and enter the key values you generated in the previous step.
* on the configuration page select **Provide your own schema** and copy paste the **kafka_datagen_credit_card_schema.json** file content into the JSON coding area
* continue creating the connector with default settings. on the final stage you can see connector active.
* once connector is active you can see the values flowing into the topic we have created.
![alt text](transaction.png)
