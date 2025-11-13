USE hotel_booking;


SELECT * FROM clients LIMIT 10;
SELECT * FROM hotels WHERE ville = 'Paris';
SELECT 
    c.nom AS client_nom, 
    h.nom AS hotel_nom, 
    r.date_debut, 
    r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
LIMIT 10;


-- Nombre de réservations par client
SELECT c.nom, COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.nom;

-- Clients qui ont réservé plus que 2 nuitées
SELECT DISTINCT c.nom
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
WHERE datediff(r.date_fin, r.date_debut) > 2;

-- Hôtels réservés par chaque client
SELECT c.nom AS client_nom, h.nom AS hotel_nom
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
GROUP BY c.nom, h.nom; -- GROUP BY pour lister les paires uniques

-- Noms des hôtels avec plus d'une réservation
SELECT h.nom, COUNT(r.reservation_id) AS total_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom
HAVING COUNT(r.reservation_id) > 1;

-- Noms des hôtels sans réservation
SELECT h.nom
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;


-- Clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT nom FROM clients 
WHERE client_id IN (
    SELECT client_id FROM reservations 
    WHERE hotel_id IN (
        SELECT hotel_id FROM hotels WHERE etoiles > 4
    )
);

-- Total des revenus générés par chaque hôtel
SELECT h.nom, SUM(r.prix_total) AS revenus_totaux
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom;


-- Revenus totaux par ville (utilise la table partitionnée)
SELECT hp.ville, SUM(r.prix_total) AS revenus_par_ville
FROM hotels_partitioned hp
JOIN reservations r ON hp.hotel_id = r.hotel_id
GROUP BY hp.ville;

-- Nombre total de réservations par client (utilise la table bucketed)
SELECT client_id, COUNT(reservation_id) AS total_reservations
FROM reservations_bucketed
GROUP BY client_id;