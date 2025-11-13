CREATE DATABASE IF NOT EXISTS hotel_booking;
USE hotel_booking;

set hive.exec.dynamic.partition=true ;
set hive.exec.dynamic.partition.mode=nonstrict ;
set hive.exec.max.dynamic.partitions=20000 ;
set hive.exec.max.dynamic.partitions.pernode=20000 ;
set hive.enforce.bucketing = true ;

CREATE TABLE clients ( client_id INT, nom STRING, email STRING,
telephone STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE hotels ( hotel_id INT, nom STRING, ville STRING,
etoiles INT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_fin DATE,
    prix_total DECIMAL(10, 2)
)
PARTITIONED BY (date_debut DATE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE; 

CREATE TABLE reservations_temp (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10, 2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE hotels_partitioned (
hotel_id INT,
nom STRING,
etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE reservations_bucketed (
reservation_id INT,
client_id INT,
hotel_id INT,
date_debut DATE,
date_fin DATE,
prix_total DECIMAL(10,2)
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;