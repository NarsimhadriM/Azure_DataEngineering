# Azure_DataEngineering
### The project is related to BBC Sports Magazine for Formula 1 rankings.

## Steps followed:

#### Step 1:
#### Access source data from Ergast and Ingest Data into the Databricks Notebooks. 
# 
#### Step 2:
#### Create ADLS Gen2 Storage on Microsoft Azure Cloud and create layers as Raw, Preparation and Presentation Layers.
# 
#### Step 3:
#### Create Azure Mounts for the files available in the ADLS Containers in the raw layer to gain access in Notebooks.
# 
#### Step 4:
#### Create custom schema to access the datasets. To view/explore the dataset and it's schema using Azure Storage Explorer.
# 
#### Step 5:
#### Read the datasets from the raw layer in a parquet format on Databricks.
# 
#### Step 6:
#### Made necessary transformations to the data like changing data types, rename attributes and adding Ingestion date column. Ingestion date column is added to have track of 
#### when the data was ingested.
# 
#### Step 7:
#### All these transformations are saved in a Dataframe. The same steps were followed for other datasets.
# 
#### Step 8:
#### Now, I have loaded the data related multiple datasets to a Preparation layer in a Parquet format.

### Step 9:
##### As we have configured the mounts for three folders. Now, access data from Join multiple datasets from the preparation layer and apply joins to meet business requirement using Databricks SQL.

### Step 10:
#### Filter the required columns/attributes to meet business requirements and set input parameters for the notebook to apply changes in the data. 
#### Now load the final dataframe to the presentation layer in a Parquet format.
