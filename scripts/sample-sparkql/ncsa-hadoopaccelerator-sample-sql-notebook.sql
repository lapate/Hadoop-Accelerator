-- Databricks notebook source
drop table IF EXISTS airportsna;
create external table airportsna (City string,
State string,
Country string,
IATA string)
location "dbfs:/mnt/demo-dataset/raw/airport-codes-na"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1');

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo-dataset/raw/

-- COMMAND ----------

-- MAGIC %fs cp dbfs:/mnt/demo-dataset/raw/airport-codes-na.txt dbfs:/mnt/demo-dataset/raw/airport-codes-na 

-- COMMAND ----------

drop table IF EXISTS departureDelays;
create external table departureDelays (date_dtm string,
delay string,
distance string,
origin string,
destination string)
USING CSV
 location "dbfs:/mnt/demo-dataset/raw/departureDelays" TBLPROPERTIES('skip.header.line.count'='1');

-- COMMAND ----------

-- MAGIC %fs cp dbfs:/mnt/demo-dataset/raw/departuredelays.csv dbfs:/mnt/demo-dataset/raw/departureDelays

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  tripIATA
  as select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a ;
CREATE OR REPLACE TEMPORARY VIEW  airports
  as select f.IATA, f.City, f.State, f.Country from airportsna f join tripIATA t on t.IATA = f.IATA;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  departureDelays_geo as 
  select 
    cast(f.date_dtm as int) as tripid, 
    cast(concat(concat(concat(concat(concat(concat('2022-', concat(concat(substr(cast(f.date_dtm as string), 1, 2), '-')), substr(cast(f.date_dtm as string), 3, 2)), ' '), substr(cast(f.date_dtm as string), 5, 2)), ':'), substr(cast(f.date_dtm as string), 7, 2)), ':00') as timestamp) as `localdate`, 
    cast(f.delay as int),
    cast(f.distance as int),
    f.origin as src, 
    f.destination as dst, 
    o.city as city_src, 
    d.city as city_dst, 
    o.state as state_src, 
    d.state as state_dst 
    from departuredelays f 
      join airports o on o.iata = f.origin 
      join airports d on d.iata = f.destination;

-- COMMAND ----------

-- MAGIC %md #### Determining the longest delay in this dataset

-- COMMAND ----------

select max(delay) from departureDelays_geo;

-- COMMAND ----------

-- MAGIC %md #### What flights departing San Antonio (SAT) are most likely to have significant delays
-- MAGIC Note, delay can be <= 0 meaning the flight left on time or early

-- COMMAND ----------

select src,dst, avg(delay) as delay from departureDelays_geo
  where src = 'SAT' and delay > 0
  group by src,dst
  order by delay DESC;
