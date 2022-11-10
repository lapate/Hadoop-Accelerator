// Databricks notebook source
// MAGIC %md # On-Time Flight Performance with Apache Spark
// MAGIC This notebook provides an analysis of On-Time Flight Performance and Departure Delays data  Apache Spark.

// COMMAND ----------

// MAGIC %md ### 0 - Pre-requisite setup
// MAGIC 
// MAGIC To get set up, do these tasks first: 
// MAGIC 
// MAGIC - Get service credentials: Client ID `<aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee>` and Client Credential `<NzQzY2QzYTAtM2I3Zi00NzFmLWI3MGMtMzc4MzRjZmk=>`. Follow the instructions in [Create service principal with portal](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal). 
// MAGIC - Get directory ID `<ffffffff-gggg-hhhh-iiii-jjjjjjjjjjjj>`: This is also referred to as *tenant ID*. Follow the instructions in [Get tenant ID](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#get-tenant-id). 
// MAGIC - If you haven't set up the service app, follow this [tutorial](https://docs.microsoft.com/en-us/azure/azure-databricks/databricks-extract-load-sql-data-warehouse). Set access at the root directory or desired folder level to the service or everyone.
// MAGIC - Create a databricks scope with 3 secrets or used previously created secrets.  As a best practice, information obtained in the setp above is wrapped in a databricks secret. These are tenantId, Application/Client Id (with Blob Data Contributor permission to ADLS Gen2 Account), and Client Secret Value of the application (client Credential) 

// COMMAND ----------

// MAGIC %md ##1 - Use Spark Configs to mount data in ADLS Gen2
// MAGIC 
// MAGIC [DBFS](https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html) mount points let you mount Azure Data Lake Store for all users in the workspace. Once it is mounted, the data can be accessed directly via a DBFS path from all clusters, without the need for providing credentials every time. The example below shows how to set up a mount point for Azure Data Lake Store.
// MAGIC 
// MAGIC With Spark configs, the Azure Data Lake Store settings can be specified per notebook. To keep things simple, the example below includes the credentials in plaintext. However, we strongly discourage you from storing secrets in plaintext. Instead, we recommend storing the credentials as [Databricks Secrets](https://docs.azuredatabricks.net/user-guide/secrets/index.html#secrets-user-guide).
// MAGIC 
// MAGIC **Note:** `spark.conf` values are visible only to the DataSet and DataFrames API. If you need access to them from an RDD, refer to the [documentation](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html#access-azure-data-lake-store-using-the-rdd-api).

// COMMAND ----------

// MAGIC %python
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="hadoopaccelratordemo ",key="secClientId"),
// MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="hadoopaccelratordemo ",key="secClientSecretValue"),
// MAGIC           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+ dbutils.secrets.get(scope="hadoopaccelratordemo ",key="secTenantId")+"/oauth2/token"}
// MAGIC 
// MAGIC 
// MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
// MAGIC 
// MAGIC mountName = 'demo-dataset'
// MAGIC mounts = [str(i) for i in dbutils.fs.ls('/mnt/')] 
// MAGIC if "FileInfo(path='dbfs:/mnt/" +mountName + "/', name='" +mountName + "/', size=0, modificationTime=0)" in mounts: 
// MAGIC   print(mountName + " has already been mounted") 
// MAGIC else: 
// MAGIC   print(mountName + " has not been mounted") 
// MAGIC   dbutils.fs.mount(
// MAGIC   source = "abfss://dataset@azdatalakestore.dfs.core.windows.net/",
// MAGIC   mount_point = "/mnt/" + mountName,
// MAGIC   extra_configs = configs)
// MAGIC   

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/demo-dataset/raw  

// COMMAND ----------

// MAGIC %md ##3 - Run SQL queries

// COMMAND ----------

// Set File Paths
val tripdelaysFilePath = "dbfs:/mnt/demo-dataset/raw/departuredelays.csv"
val airportsnaFilePath = "dbfs:/mnt/demo-dataset/raw/airport-codes-na.txt"


// Obtain airports dataset
// Note that "spark-csv" package is built-in datasource in Spark 2.0
val airportsna = sqlContext.read.format("com.databricks.spark.csv").
  option("header", "true").
  option("inferschema", "true").
  option("delimiter", "\t").
  load(airportsnaFilePath)

airportsna.createOrReplaceTempView("airports_na")

// Obtain departure Delays data
val departureDelays = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(tripdelaysFilePath)
departureDelays.createOrReplaceTempView("departureDelays")
departureDelays.cache()

// Available IATA (International Air Transport Association) codes from the departuredelays sample dataset
val tripIATA = sqlContext.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.createOrReplaceTempView("tripIATA")

// Only include airports with atleast one trip from the departureDelays dataset
val airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.createOrReplaceTempView("airports")
airports.cache()

// Build `departureDelays_geo` DataFrame
// Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
val departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2022-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination") 

// RegisterTempTable
departureDelays_geo.createOrReplaceTempView("departureDelays_geo")

// Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()

// COMMAND ----------

// MAGIC %md #### Determining the longest delay in this dataset

// COMMAND ----------

// Finding the longest Delay
val longestDelay = departureDelays_geo.groupBy().max("delay")
display(longestDelay)

// COMMAND ----------

// MAGIC %md #### What flights departing San Antonio (SAT) are most likely to have significant delays
// MAGIC Note, delay can be <= 0 meaning the flight left on time or early

// COMMAND ----------


val satDelayedTrips = departureDelays_geo.
  filter("src = 'SAT' and delay > 0").
  groupBy("src", "dst").
  avg("delay").
  orderBy(org.apache.spark.sql.functions.col("avg(delay)").desc)

display(satDelayedTrips)
