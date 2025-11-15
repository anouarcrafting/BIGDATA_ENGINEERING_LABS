-- Chargement du dataset

flights = LOAD 'flights' USING PigStorage(',') AS (
    year:int, month:int, daymonth:int, dayweek:int,
    deptime:int, crsdeptime:int, arrtime:int, crsarrtime:int,
    carrier:chararray, flightnum:int, tailnum:chararray,
    actualelapsed:int, crselapsed:int, airtime:int,
    arrdelay:int, depdelay:int,
    origin:chararray, dest:chararray, distance:int,
    taxiin:int, taxiout:int,
    cancelled:int, cancellationcode:chararray, diverted:int,
    carrierdelay:int, weatherdelay:int, nasdelay:int,
    securitydelay:int, lateaircraftdelay:int
);


-- 1. Top 20 aéroports par volume total de vols


out = GROUP flights BY origin;
out_count = FOREACH out GENERATE group AS airport, COUNT(flights) AS nb_out;

in = GROUP flights BY dest;
in_count = FOREACH in GENERATE group AS airport, COUNT(flights) AS nb_in;

all = JOIN out_count BY airport LEFT OUTER, in_count BY airport;

traffic = FOREACH all GENERATE
    out_count::airport AS airport,
    nb_out,
    nb_in,
    (nb_out + nb_in) AS total;

top20_airports = LIMIT (ORDER traffic BY total DESC) 20;


-- 2. Trafic par année


grp_year = GROUP flights BY year;
traffic_year = FOREACH grp_year GENERATE group AS year, COUNT(flights) AS total;


-- 3. Trafic par mois


grp_month = GROUP flights BY (year, month);
traffic_month = FOREACH grp_month GENERATE
    group.year, group.month, COUNT(flights) AS total;


-- 4. Trafic par jour


grp_day = GROUP flights BY (year, month, daymonth);
traffic_day = FOREACH grp_day GENERATE
    group.year, group.month, group.daymonth, COUNT(flights) AS total;


-- 5. Popularité des transporteurs (log10 du volume annuel)


grp_carrier = GROUP flights BY (carrier, year);
count_carrier = FOREACH grp_carrier GENERATE
    group.carrier AS carrier,
    group.year AS year,
    COUNT(flights) AS total;

carrier_log = FOREACH count_carrier GENERATE
    carrier, year, LOG10(total) AS log_total;


-- 6. Proportion de vols retardés (retard > 15 min)


delay = FOREACH flights GENERATE
    year, month, daymonth,
    (arrdelay > 15 ? 1 : 0) AS isDelayed;

-- Par année
grp_delay_year = GROUP delay BY year;
prop_delay_year = FOREACH grp_delay_year GENERATE
    group AS year,
    AVG(delay.isDelayed) AS proportion;

-- Par mois
grp_delay_month = GROUP delay BY (year, month);
prop_delay_month = FOREACH grp_delay_month GENERATE
    group.year, group.month,
    AVG(delay.isDelayed) AS proportion;

-- Par jour
grp_delay_day = GROUP delay BY (year, month, daymonth);
prop_delay_day = FOREACH grp_delay_day GENERATE
    group.year, group.month, group.daymonth,
    AVG(delay.isDelayed) AS proportion;


-- 7. Retards par transporteur (retard > 15 min)


carrierD = FOREACH flights GENERATE
    carrier, year, month, daymonth,
    (arrdelay > 15 ? 1 : 0) AS isDelayed;

grp_cy = GROUP carrierD BY (carrier, year);
prop_carrier = FOREACH grp_cy GENERATE
    group.carrier, group.year,
    AVG(carrierD.isDelayed) AS proportion;


-- 8. Itinéraires les plus fréquentés (paire non ordonnée)


routes = FOREACH flights GENERATE
    (origin < dest ? origin : dest) AS a,
    (origin < dest ? dest : origin)  AS b;

grp_routes = GROUP routes BY (a, b);
freq_routes = FOREACH grp_routes GENERATE
    group.a AS origin,
    group.b AS dest,
    COUNT(routes) AS total;

top_routes = ORDER freq_routes BY total DESC;