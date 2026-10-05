-- ============================================================================
-- verifications_raw.sql : preuves que la couche RAW est correcte (jour 2)
--
-- Partie A : les objets existent et appartiennent au rôle des outils.
-- Partie B : après le chargement de janvier, RAW est une copie fidèle du fichier.
-- Partie C : historique de chargement (la preuve qu'un fichier n'est chargé qu'une fois).
-- Partie D : les zones.
--
-- Chaque requête se lance seule (Ctrl/Cmd + Entrée) : Snowsight n'affiche que le
-- résultat de la dernière instruction exécutée.
-- ============================================================================
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE NYC_TAXI_WH;
USE SCHEMA NYC_TAXI.RAW;


-- ----------------------------------------------------------------------------
-- A. Les objets de RAW et leur propriétaire
-- Attendu : 5 lignes, proprietaire = TRANSFORMER partout ;
--           YELLOW_TRIPDATA = 22 colonnes, TAXI_ZONE_LOOKUP = 6 colonnes.
-- Un autre propriétaire (ACCOUNTADMIN) rendrait l'objet invisible pour les outils.
-- ----------------------------------------------------------------------------
SELECT 'TABLE' AS type, t.table_name AS nom, t.table_owner AS proprietaire,
       (SELECT COUNT(*) FROM NYC_TAXI.INFORMATION_SCHEMA.COLUMNS c
         WHERE c.table_schema = 'RAW' AND c.table_name = t.table_name) AS colonnes
FROM NYC_TAXI.INFORMATION_SCHEMA.TABLES t
WHERE t.table_schema = 'RAW'
UNION ALL
SELECT 'STAGE', stage_name, stage_owner, NULL
FROM NYC_TAXI.INFORMATION_SCHEMA.STAGES WHERE stage_schema = 'RAW'
UNION ALL
SELECT 'FILE FORMAT', file_format_name, file_format_owner, NULL
FROM NYC_TAXI.INFORMATION_SCHEMA.FILE_FORMATS WHERE file_format_schema = 'RAW';


-- ----------------------------------------------------------------------------
-- B. Après le chargement de janvier 2025
-- ----------------------------------------------------------------------------

-- B1. D'où viennent les lignes ?
-- Attendu : une seule ligne, yellow_tripdata_2025-01.parquet (sans dossier), 3 475 226 lignes.
SELECT _source_file, COUNT(*) AS lignes, MIN(_loaded_at) AS premier_chargement, MAX(_loaded_at) AS dernier_chargement
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
GROUP BY 1;

-- B2. Copie fidèle : mêmes totaux que le fichier Parquet (mesurés avec DuckDB).
-- Attendu :
--   lignes 3475226 ; somme_total_amount 89005026.80 ; somme_fare_amount 59363125.08
--   somme_trip_distance 20347886.73 ; somme_tip 10286018.35
--   min_pickup 2024-12-31 20:47:55 ; max_pickup 2025-02-01 00:00:44
--   passagers_null 540149 ; distance_zero 90893 ; fare_negatif 144118
-- Les sommes sont des flottants : un écart de quelques centimes est normal. Un écart
-- de plusieurs unités révèle des montants arrondis (NUMBER sans décimales).
-- Des dates en 1970 signalent un format Parquet sans USE_LOGICAL_TYPE.
SELECT COUNT(*)                         AS lignes,
       ROUND(SUM(total_amount), 2)      AS somme_total_amount,
       ROUND(SUM(fare_amount), 2)       AS somme_fare_amount,
       ROUND(SUM(trip_distance), 2)     AS somme_trip_distance,
       ROUND(SUM(tip_amount), 2)        AS somme_tip,
       MIN(tpep_pickup_datetime)        AS min_pickup,
       MAX(tpep_pickup_datetime)        AS max_pickup,
       COUNT_IF(passenger_count IS NULL) AS passagers_null,
       COUNT_IF(trip_distance = 0)      AS distance_zero,
       COUNT_IF(fare_amount < 0)        AS fare_negatif
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA;

-- B3. Toutes les colonnes sont-elles remplies ? Une colonne entièrement vide alors
-- qu'elle est remplie dans le fichier a un nom différent de celui du fichier.
SELECT * FROM NYC_TAXI.RAW.YELLOW_TRIPDATA LIMIT 5;


-- ----------------------------------------------------------------------------
-- C. Historique de chargement (à capturer pour les livrables)
-- Attendu après deux chargements de janvier : UNE seule ligne par fichier, statut LOADED.
-- Un second COPY INTO ne ressort pas ici : Snowflake n'a rien chargé.
-- ----------------------------------------------------------------------------
SELECT file_name, status, row_parsed, row_count, last_load_time
FROM TABLE(NYC_TAXI.INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'YELLOW_TRIPDATA',
       START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time;


-- ----------------------------------------------------------------------------
-- D. Les zones
-- ----------------------------------------------------------------------------

-- D1. Attendu : 265 lignes, 265 identifiants distincts.
SELECT COUNT(*) AS lignes, COUNT(DISTINCT locationid) AS ids_distincts, MIN(locationid) AS id_min, MAX(locationid) AS id_max
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP;

-- D2. Les « N/A » sont du texte, pas des NULL : zone 264 (Unknown) et 265 (Outside of NYC).
SELECT * FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP WHERE locationid IN (264, 265);
