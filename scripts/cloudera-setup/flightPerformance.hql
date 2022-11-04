use demo;
CREATE EXTERNAL TABLE IF NOT EXISTS airportsna ( 
   City string,
State string,
Country string,
IATA string)
COMMENT 'airport information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
TBLPROPERTIES('transactional'='true', 'skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS departureDelays ( 
   date_dtm string,
delay string,
distance string,
origin string,
destination string)
COMMENT 'delay information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES('transactional'='true', 'skip.header.line.count'='1');


DROP TABLE IF EXISTS tripIATA;
CREATE TEMPORARY TABLE  tripIATA
  as select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a ;
DROP TABLE IF EXISTS airports;  
CREATE TEMPORARY TABLE  airports
  as select f.IATA, f.City, f.State, f.Country from airportsna f join tripIATA t on t.IATA = f.IATA;
  
  
  CREATE TEMPORARY TABLE departureDelays_geo
   as
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