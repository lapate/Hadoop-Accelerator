// Databricks notebook source
// MAGIC %md # On-Time Flight Performance with Apache Spark
// MAGIC This notebook provides an analysis of On-Time Flight Performance and Departure Delays data  Apache Spark.

// COMMAND ----------

// MAGIC %md ### Preparation
// MAGIC Extract the Airports and Departure Delays information from adls

// COMMAND ----------


// Set File Paths
val tripdelaysFilePath = "dbfs:/FileStore/shared_uploads/jaseemhamsa@microsoft.com/flights/departuredelays.csv"
val airportsnaFilePath = "dbfs:/FileStore/shared_uploads/jaseemhamsa@microsoft.com/flights/airport_codes_na.txt"

// COMMAND ----------


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

// COMMAND ----------


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

// MAGIC %md #### What flights departing SAn Antonio (SAT) are most likely to have significant delays
// MAGIC Note, delay can be <= 0 meaning the flight left on time or early

// COMMAND ----------


val satDelayedTrips = departureDelays_geo.
  filter("src = 'SAT' and delay > 0").
  groupBy("src", "dst").
  avg("delay").
  orderBy(org.apache.spark.sql.functions.col("avg(delay)").desc)

// COMMAND ----------

display(sfoDelayedTrips)

// COMMAND ----------


