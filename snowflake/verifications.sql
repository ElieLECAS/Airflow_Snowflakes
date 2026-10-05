-- ============================================================================
-- verifications.sql : preuves que l'entrepôt et les droits sont corrects
--
-- Partie A : inventaire (à lancer avec un rôle administrateur).
-- Partie B : tests avec le rôle des outils. Les tests qui doivent RÉUSSIR
--            se lancent d'un bloc ; ceux qui doivent ÉCHOUER se lancent un par un
--            (Ctrl/Cmd + Entrée) : Snowsight s'arrête à la première erreur.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- A. Inventaire
-- ----------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

SHOW ROLES LIKE 'TRANSFORMER';                    -- colonne owner : USERADMIN
SHOW GRANTS OF ROLE TRANSFORMER;                  -- donné à SYSADMIN et à AIRFLOW_SVC
SHOW GRANTS TO ROLE TRANSFORMER;                  -- tous les droits du rôle : rien de plus que prévu
SHOW GRANTS TO USER AIRFLOW_SVC;                  -- le rôle TRANSFORMER, et lui seul
DESC USER AIRFLOW_SVC;                            -- TYPE = SERVICE ; RSA_PUBLIC_KEY_FP : à comparer à l'empreinte du poste

USE ROLE SYSADMIN;

SHOW WAREHOUSES LIKE 'NYC_TAXI_WH';               -- size = X-Small, auto_suspend = 60, auto_resume = true
SHOW WAREHOUSES;                                  -- le brief demande un seul warehouse XS (voir COMPUTE_WH)
SHOW SCHEMAS IN DATABASE NYC_TAXI;                -- RAW, STAGING, INTERMEDIATE, MARTS (+ INFORMATION_SCHEMA, PUBLIC)


-- ----------------------------------------------------------------------------
-- B. Tests avec le rôle des outils
-- ----------------------------------------------------------------------------

-- Les rôles secondaires s'ajoutent au rôle actif : sur un compte d'essai ils peuvent
-- garder les droits d'ACCOUNTADMIN et fausser les tests. On les coupe.
USE SECONDARY ROLES NONE;
USE ROLE TRANSFORMER;
USE WAREHOUSE NYC_TAXI_WH;

SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();  -- TRANSFORMER, et aucun rôle secondaire

-- B1. Ce qui doit RÉUSSIR (à lancer d'un bloc)
CREATE TABLE NYC_TAXI.RAW.TEST_DROITS (id INT);
CREATE FILE FORMAT NYC_TAXI.RAW.TEST_FORMAT TYPE = CSV;
CREATE STAGE NYC_TAXI.RAW.TEST_STAGE;
CREATE TABLE NYC_TAXI.STAGING.TEST_DROITS (id INT);
CREATE VIEW  NYC_TAXI.STAGING.TEST_VUE AS SELECT 1 AS id;
CREATE TABLE NYC_TAXI.INTERMEDIATE.TEST_DROITS (id INT);
CREATE TABLE NYC_TAXI.MARTS.TEST_DROITS (id INT);
INSERT INTO NYC_TAXI.MARTS.TEST_DROITS VALUES (1);
SHOW TABLES LIKE 'TEST_DROITS' IN DATABASE NYC_TAXI;      -- owner = TRANSFORMER dans les quatre schémas

-- Nettoyage : le rôle est propriétaire, il peut supprimer ses objets.
DROP TABLE  NYC_TAXI.RAW.TEST_DROITS;
DROP FILE FORMAT NYC_TAXI.RAW.TEST_FORMAT;
DROP STAGE  NYC_TAXI.RAW.TEST_STAGE;
DROP VIEW   NYC_TAXI.STAGING.TEST_VUE;
DROP TABLE  NYC_TAXI.STAGING.TEST_DROITS;
DROP TABLE  NYC_TAXI.INTERMEDIATE.TEST_DROITS;
DROP TABLE  NYC_TAXI.MARTS.TEST_DROITS;

-- B2. Ce qui doit ÉCHOUER (une instruction à la fois)
CREATE SCHEMA NYC_TAXI.AUTRE;                              -- pas de CREATE SCHEMA : « Insufficient privileges »
CREATE DATABASE AUTRE_BASE;                                -- pas de CREATE DATABASE
CREATE WAREHOUSE AUTRE_WH;                                 -- pas de CREATE WAREHOUSE
ALTER WAREHOUSE NYC_TAXI_WH SET WAREHOUSE_SIZE = 'LARGE';  -- pas de MODIFY : impossible de monter en taille
CREATE ROLE AUTRE_ROLE;                                    -- pas de CREATE ROLE
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1;  -- pas d'accès à la facturation

-- Destructif si le refus n'avait pas lieu : à ne lancer qu'après avoir vérifié que le
-- rôle actif est TRANSFORMER, sans rôle secondaire (SELECT CURRENT_SECONDARY_ROLES()).
-- DROP DATABASE NYC_TAXI;                                 -- pas propriétaire de la base
