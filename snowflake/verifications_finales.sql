-- ============================================================================
-- verifications_finales.sql : contrôles du jour 5
--
--   A. Les trajets anormaux d'un mois, comptés à la main, comparés à MART_DATA_QUALITY
--   B. Les crédits consommés (rôle ACCOUNTADMIN, à lancer par une personne)
--   C. Les droits du rôle des outils, et ce qu'il n'a pas le droit de faire
--
-- Chaque requête se lance seule (Ctrl/Cmd + Entrée) : Snowsight n'affiche que le résultat de la
-- dernière instruction exécutée.
-- ============================================================================


-- ============================================================================
-- A. Trajets anormaux : janvier 2025
-- ============================================================================
USE ROLE TRANSFORMER;
USE WAREHOUSE NYC_TAXI_WH;

-- A1. Chaque règle comptée SEULE. Une même ligne peut échouer à plusieurs règles à la fois :
-- la somme de ces colonnes dépasse donc le nombre de trajets écartés.
-- Les seuils sont ceux des paramètres du DAG : 100 miles, 180 minutes.
SELECT
    COUNT(*)                                                                           AS total,
    COUNT_IF(pickup_at IS NULL OR dropoff_at IS NULL)                                  AS timestamp_null,
    COUNT_IF(dropoff_at <= pickup_at)                                                  AS duration_non_positive,
    COUNT_IF(DATEDIFF('second', pickup_at, dropoff_at) > 180 * 60)                     AS duration_too_long,
    COUNT_IF(DATE_TRUNC('month', pickup_at) <> source_file_month)                      AS pickup_outside_file_month,
    COUNT_IF(trip_distance_miles <= 0 OR trip_distance_miles > 100)                    AS distance_out_of_range,
    COUNT_IF(fare_amount < 0 OR total_amount <= 0)                                     AS amount_non_positive,
    COUNT_IF(pickup_zone_key IS NULL OR dropoff_zone_key IS NULL)                      AS zone_null
FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
WHERE source_file_month = '2025-01-01';

-- A2. Les trajets qui échouent à AU MOINS UNE règle, comptés une seule fois chacun.
-- Ce nombre doit être le même que le total écarté par le pipeline (voir A3).
SELECT COUNT(*) AS trajets_ecartes
FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
WHERE source_file_month = '2025-01-01'
  AND (   pickup_at IS NULL OR dropoff_at IS NULL
       OR dropoff_at <= pickup_at
       OR DATEDIFF('second', pickup_at, dropoff_at) > 180 * 60
       OR DATE_TRUNC('month', pickup_at) <> source_file_month
       OR trip_distance_miles <= 0 OR trip_distance_miles > 100
       OR fare_amount < 0 OR total_amount <= 0
       OR pickup_zone_key IS NULL OR dropoff_zone_key IS NULL);

-- A3. Ce que dit le pipeline : UNE seule raison par trajet, la première règle qui échoue
-- (l'ordre est celui de int_trips__flagged.sql). Ici la somme des lignes écartées est le total écarté.
SELECT status, nb_rows, pct_of_file
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
WHERE source_file_month = '2025-01-01'
ORDER BY nb_rows DESC;


-- ============================================================================
-- B. Crédits consommés
-- À lancer avec ACCOUNTADMIN : le rôle des outils n'a pas le droit de lire la facturation.
-- SNOWFLAKE.ACCOUNT_USAGE a jusqu'à environ 3 heures de retard : les dernières heures peuvent manquer.
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- B1. Crédits par warehouse et par jour, sur les 10 derniers jours.
SELECT warehouse_name,
       DATE(start_time)                AS jour,
       ROUND(SUM(credits_used), 4)     AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -10, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2, 1;

-- B2. Total par warehouse sur la même période.
SELECT warehouse_name,
       ROUND(SUM(credits_used), 4)     AS credits_total
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -10, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================================================
-- C. Droits du rôle des outils
-- ============================================================================
USE ROLE SECURITYADMIN;

-- C1. Résumé : combien de droits de chaque sorte, sur quels types d'objets.
SHOW GRANTS TO ROLE TRANSFORMER;
SELECT "privilege", "granted_on", COUNT(*) AS nombre
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
GROUP BY 1, 2
ORDER BY 2, 1;

-- C2. Les droits qui ne sont pas des OWNERSHIP : ce que le rôle a reçu explicitement.
-- Tout le reste (OWNERSHIP) vient de ce qu'il a créé lui-même.
SHOW GRANTS TO ROLE TRANSFORMER;
SELECT "privilege", "granted_on", "name"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "privilege" <> 'OWNERSHIP'
ORDER BY 2, 3, 1;

-- C3. Ce que le rôle n'a PAS le droit de faire (une instruction à la fois, avec le rôle des outils).
USE SECONDARY ROLES NONE;
USE ROLE TRANSFORMER;
USE WAREHOUSE NYC_TAXI_WH;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1;  -- pas d'accès à la facturation
CREATE SCHEMA NYC_TAXI.AUTRE;                                              -- pas de CREATE SCHEMA
ALTER WAREHOUSE NYC_TAXI_WH SET AUTO_SUSPEND = 60;                         -- pas de MODIFY sur le warehouse (valeur déjà en place : sans effet si la commande passait)
