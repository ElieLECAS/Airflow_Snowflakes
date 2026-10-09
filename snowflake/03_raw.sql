-- ============================================================================
-- 03_raw.sql : objets de la couche RAW (jour 2)
--
-- Crée dans NYC_TAXI.RAW deux formats de fichier, un stage et les deux tables
-- décrites par le contrat RAW du brief. Aucune donnée n'est chargée ici : le chargement
-- (PUT puis COPY INTO) est fait par ingestion/charger.py.
--
-- À exécuter en entier dans Snowsight (Run All), avec le rôle des outils :
-- le rôle qui crée un objet en devient propriétaire, et un objet créé par
-- ACCOUNTADMIN serait invisible pour Airflow et pour le script Python.
--
-- Rejouable :
--   - les formats de fichier sont de la configuration pure : CREATE OR REPLACE ;
--   - le stage et les tables contiennent des données : CREATE ... IF NOT EXISTS,
--     un OR REPLACE supprimerait les fichiers déposés ou les lignes chargées.
-- Conséquence : si la définition d'une table change un jour, IF NOT EXISTS ne la
-- modifie pas ; il faut un ALTER TABLE (ou vider et recréer la table).
-- ============================================================================
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;          -- seuls les droits de TRANSFORMER s'appliquent
USE WAREHOUSE NYC_TAXI_WH;


-- ----------------------------------------------------------------------------
-- 1. Formats de fichier : comment Snowflake lit chaque fichier
-- ----------------------------------------------------------------------------

-- Trajets : un Parquet contient déjà les noms et les types de ses colonnes.
-- USE_LOGICAL_TYPE = TRUE : lit les dates du fichier comme des dates et non comme
-- des nombres. À vérifier après le chargement (MIN et MAX de tpep_pickup_datetime).
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.PARQUET_FF
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

-- Zones : CSV avec en-tête et valeurs entre guillemets.
-- ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE : la table a deux colonnes de plus que le
-- fichier (_source_file et _loaded_at), remplies par COPY INTO.
-- NULL_IF n'est pas modifié : les « N/A » des zones 264 et 265 restent du texte.
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.CSV_FF
  TYPE = CSV
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- ----------------------------------------------------------------------------
-- 2. Stage : zone de dépôt des fichiers avant leur copie dans une table
--
-- Les fichiers sont déposés à la racine du stage : _source_file contiendra alors
-- le nom seul (yellow_tripdata_2025-01.parquet), sans dossier devant.
-- ----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS NYC_TAXI.RAW.TLC_STAGE
  COMMENT = 'Dépôt des fichiers TLC (Parquet des trajets, CSV des zones) avant COPY INTO';


-- ----------------------------------------------------------------------------
-- 3. Tables RAW : copie fidèle des fichiers
--
-- Types larges, jamais déduits d'un seul fichier : d'un mois à l'autre une même
-- colonne peut passer d'entière à décimale.
--   - identifiants et compteurs : NUMBER (sans décimales, c'est voulu) ;
--   - montants et distances     : FLOAT (un NUMBER seul arrondirait à l'entier) ;
--   - dates sans fuseau         : TIMESTAMP_NTZ ;
--   - texte                     : VARCHAR.
-- Les noms des colonnes sont ceux des fichiers TLC. COPY INTO les associe par nom,
-- sans tenir compte des majuscules (Airport_fee dans le fichier, airport_fee ici).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.YELLOW_TRIPDATA (
  vendorid               NUMBER,
  tpep_pickup_datetime   TIMESTAMP_NTZ,
  tpep_dropoff_datetime  TIMESTAMP_NTZ,
  passenger_count        NUMBER,
  trip_distance          FLOAT,
  ratecodeid             NUMBER,
  store_and_fwd_flag     VARCHAR,
  pulocationid           NUMBER,
  dolocationid           NUMBER,
  payment_type           NUMBER,
  fare_amount            FLOAT,
  extra                  FLOAT,
  mta_tax                FLOAT,
  tip_amount             FLOAT,
  tolls_amount           FLOAT,
  improvement_surcharge  FLOAT,
  total_amount           FLOAT,
  congestion_surcharge   FLOAT,
  airport_fee            FLOAT,
  cbd_congestion_fee     FLOAT,
  -- colonnes techniques, absentes des fichiers : remplies par COPY INTO
  _source_file           VARCHAR,        -- nom du fichier d'origine
  _loaded_at             TIMESTAMP_NTZ   -- date et heure du chargement
)
COMMENT = 'RAW : un trajet par ligne, copie fidèle des fichiers Parquet mensuels de la TLC';

CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.TAXI_ZONE_LOOKUP (
  locationid    NUMBER,                  -- unique (non imposé par Snowflake)
  borough       VARCHAR,
  zone          VARCHAR,
  service_zone  VARCHAR,
  _source_file  VARCHAR,
  _loaded_at    TIMESTAMP_NTZ
)
COMMENT = 'RAW : les 265 zones de taxi de la TLC, chargées une seule fois';

-- Vérifications (propriétaire, colonnes) : snowflake/verifications_raw.sql
