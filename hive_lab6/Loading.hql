LOAD DATA LOCAL INPATH '/path/to/clients.txt' INTO TABLE clients;
LOAD DATA LOCAL INPATH '/path/to/hotels.txt' INTO TABLE hotels;

LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' INTO TABLE reservations_temp;



INSERT INTO TABLE reservations PARTITION(date_debut)
SELECT 
    reservation_id, 
    client_id, 
    hotel_id, 
    date_fin, 
    prix_total, 
    date_debut  -- La colonne de partition doit être la dernière du SELECT
FROM reservations_temp;

INSERT INTO TABLE hotels_partitioned PARTITION(ville)
SELECT 
    hotel_id, 
    nom, 
    etoile, 
    
    ville  
FROM hotels;

INSERT INTO TABLE reservations_bucketed
SELECT 
    reservation_id, 
    client_id, 
    hotel_id, 
    date_debut, 
    date_fin, 
    prix_total 
FROM reservations;