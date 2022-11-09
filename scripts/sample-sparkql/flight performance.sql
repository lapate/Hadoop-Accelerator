-- Databricks notebook source
drop table airportsna;
create external table airportsna (City string,
State string,
Country string,
IATA string)
location 'dbfs:/FileStore/shared_uploads/jaseemhamsa@microsoft.com/airportsna/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1');

-- COMMAND ----------

drop table departureDelays;
create external table departureDelays (date_dtm string,
delay string,
distance string,
origin string,
destination string)
USING CSV
 location 'dbfs:/FileStore/shared_uploads/jaseemhamsa@microsoft.com/departureDelays/' TBLPROPERTIES('skip.header.line.count'='1');

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

select max(delay) from departureDelays_geo;

-- COMMAND ----------

select src,dst, avg(delay) as delay from departureDelays_geo
  where src = 'SAT' and delay > 0
  group by src,dst
  order by delay DESC;

-- COMMAND ----------

groupBy("src", "dst")filter("src = 'SAT' and delay > 0").
  groupBy("src", "dst").
  avg("delay").
  orderBy(org.apache.spark.sql.functions.col("avg(delay)").desc

-- COMMAND ----------


