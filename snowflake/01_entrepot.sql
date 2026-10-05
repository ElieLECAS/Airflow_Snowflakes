-- ============================================================================
-- 01_entrepot.sql : entrepôt NYC Yellow Taxi (jour 1)
--
-- Crée le warehouse, la base, les quatre schémas, le rôle des outils et
-- l'utilisateur de service. Rejouable : chaque création est suivie d'un ALTER
-- qui remet l'objet dans l'état voulu s'il existait déjà.
--
-- À exécuter en entier dans Snowsight (flèche à côté de « Run » puis « Run All »).
-- Ordre imposé par les dépendances :
--   rôle -> warehouse -> base -> schémas -> droits -> utilisateur -> rôle donné à l'utilisateur
--
-- La clé publique de l'utilisateur est enregistrée à part (02_cle_publique.sql).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Le rôle des outils (USERADMIN : crée les rôles et les utilisateurs)
-- ----------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORMER
  COMMENT = 'Rôle des outils (Airflow, scripts Python) : chargement, transformations, contrôles';

-- SYSADMIN hérite du rôle : les objets créés par TRANSFORMER restent visibles
-- et gérables par les administrateurs.
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;


-- ----------------------------------------------------------------------------
-- 2. Warehouse, base et schémas (SYSADMIN : crée les objets de calcul et de stockage)
-- ----------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60            -- secondes d'inactivité avant suspension
  AUTO_RESUME = TRUE           -- redémarre seul à la première requête
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Calcul du pipeline NYC Taxi : taille XS, suspendu après 60 s';

-- Si le warehouse existait déjà avec d'autres réglages, on les remet.
ALTER WAREHOUSE NYC_TAXI_WH SET
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS NYC_TAXI
  COMMENT = 'Pipeline médaillon NYC Yellow Taxi (janvier à mars 2025)';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW          COMMENT = 'Bronze : copie fidèle des fichiers';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.STAGING      COMMENT = 'Argent : colonnes renommées et typées';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.INTERMEDIATE COMMENT = 'Trajets valides ou rejetés, puis enrichis';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.MARTS        COMMENT = 'Or : table de faits, dimensions, tables d''analyse';


-- ----------------------------------------------------------------------------
-- 3. Droits du rôle des outils (moindre privilège)
--
-- SYSADMIN est propriétaire des objets qu'il vient de créer : il peut donc
-- donner des droits dessus. Pas de SELECT, INSERT ni droits futurs : TRANSFORMER
-- sera propriétaire de chaque table et vue qu'il crée, donc il a déjà tous les
-- droits dessus (y compris CREATE OR REPLACE).
-- ----------------------------------------------------------------------------

-- Calcul : USAGE suffit, AUTO_RESUME démarre le warehouse. Pas d'OPERATE,
-- pas de MODIFY : le rôle ne peut pas changer la taille ni la suspension.
GRANT USAGE ON WAREHOUSE NYC_TAXI_WH TO ROLE TRANSFORMER;

-- Chaîne d'accès : base, puis schémas. Pas de CREATE SCHEMA : les schémas
-- viennent de ce script, les outils n'en créent pas.
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE TRANSFORMER;

-- RAW : tables du chargement, stage interne (PUT) et formats de fichier (COPY INTO).
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA NYC_TAXI.RAW TO ROLE TRANSFORMER;

-- STAGING : deux vues de renommage, mais aussi trois tables de codes (codes_tlc.sql).
-- INTERMEDIATE et MARTS : les fichiers fournis y créent des tables ; CREATE VIEW
-- est demandé par le contrat de la couche RAW.
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.STAGING      TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.MARTS        TO ROLE TRANSFORMER;


-- ----------------------------------------------------------------------------
-- 4. L'utilisateur de service (USERADMIN)
--
-- TYPE = SERVICE : pas de mot de passe, pas d'accès à l'interface web,
-- authentification par paire de clés uniquement.
-- DEFAULT_ROLE indique le rôle activé à la connexion mais ne le donne pas :
-- c'est le GRANT ROLE qui le donne.
-- ----------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS AIRFLOW_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = NYC_TAXI_WH
  DEFAULT_NAMESPACE = NYC_TAXI.RAW
  COMMENT = 'Compte de service des outils : scripts Python et Airflow';

-- Si l'utilisateur existait déjà, on remet ses valeurs par défaut.
ALTER USER AIRFLOW_SVC SET
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = NYC_TAXI_WH
  DEFAULT_NAMESPACE = NYC_TAXI.RAW;

GRANT ROLE TRANSFORMER TO USER AIRFLOW_SVC;


-- ----------------------------------------------------------------------------
-- 5. Les warehouses créés d'office par le compte d'essai
--
-- Contrainte du brief : warehouses de taille XS, suspendus après 60 s au plus.
-- Un compte d'essai en crée trois d'office : COMPUTE_WH et SNOWFLAKE_LEARNING_WH
-- (suspension à 300 s) et SYSTEM$STREAMLIT_NOTEBOOK_WH (géré par le système, déjà
-- à 60 s). On règle les deux premiers plutôt que de les supprimer : c'est réversible
-- et les feuilles de Snowsight utilisent COMPUTE_WH par défaut. Le pipeline,
-- lui, n'utilise que NYC_TAXI_WH.
--
-- ACCOUNTADMIN est utilisé ici par une personne, jamais par les outils.
-- IF EXISTS : le script reste rejouable sur un compte où ils n'existent pas.
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

ALTER WAREHOUSE IF EXISTS COMPUTE_WH           SET WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
ALTER WAREHOUSE IF EXISTS SNOWFLAKE_LEARNING_WH SET WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
