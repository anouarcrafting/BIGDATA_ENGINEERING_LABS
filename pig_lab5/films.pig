-- Charger PiggyBank pour JSONLoader
REGISTER /path/to/piggybank.jar;

-- Charger les films
films = LOAD '/input/films.json' 
        USING org.apache.pig.piggybank.storage.JsonLoader() 
        AS (id:chararray, title:chararray, year:int, genre:chararray, summary:chararray, 
            country:chararray, director:map[], actors:bag{T:tuple(_id:chararray, role:chararray)});

-- Charger les artistes
artists = LOAD '/input/artists.json' 
          USING org.apache.pig.piggybank.storage.JsonLoader() 
          AS (id:chararray, last_name:chararray, first_name:chararray, birth_date:chararray);

-- Filtrer films américains
films_us = FILTER films BY country == 'USA';

-- Grouper par année
mUSA_annee = GROUP films_us BY year;

-- Grouper par réalisateur
mUSA_director = GROUP films_us BY director#'_id';

-- Triplets idFilm, idActeur, role
mUSA_acteurs = FOREACH films_us GENERATE id AS idFilm, FLATTEN(actors) AS actor;
mUSA_acteurs = FOREACH mUSA_acteurs GENERATE idFilm, actor#'_id' AS idActeur, actor#'role' AS role;

-- Associer idFilm à la description complète de l’acteur
moviesActors = JOIN mUSA_acteurs BY idActeur, artists BY id;
moviesActors = FOREACH moviesActors GENERATE mUSA_acteurs.idFilm AS idFilm,
                                       artists.id AS idActeur,
                                       artists.first_name,
                                       artists.last_name,
                                       artists.birth_date;

-- Description complète du film + acteurs
fullMovies_cogroup = COGROUP films_us BY id, moviesActors BY idFilm;
fullMovies = FOREACH fullMovies_cogroup GENERATE
                group AS idFilm,
                FLATTEN(films_us) AS film,
                moviesActors AS actors;

--  Films joués par l’acteur
films_played = FOREACH moviesActors GENERATE idActeur AS idArtiste, idFilm, role;

--  Films dirigés par l’acteur
films_directed = FOREACH films_us GENERATE director#'_id' AS idArtiste, id AS idFilm, title;

--  ActeursRealisateurs : cogroup artistes, films joués, films dirigés
ActeursRealisateurs_cg = COGROUP artists BY id LEFT OUTER, films_played BY idArtiste LEFT OUTER, films_directed BY idArtiste;
ActeursRealisateurs = FOREACH ActeursRealisateurs_cg GENERATE
                        artists.id AS idArtiste,
                        artists.first_name,
                        artists.last_name,
                        films_played AS filmsJoues,
                        films_directed AS filmsDiriges;

--  Stocker le résultat
STORE ActeursRealisateurs INTO '/pigout/ActeursRealisateurs' USING PigStorage(',');