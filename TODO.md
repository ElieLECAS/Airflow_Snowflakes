# TODO : Pipeline médaillon NYC Yellow Taxi (Snowflake + Airflow)

Cocher au fur et à mesure : `- [ ]` devient `- [x]`.
Environnement retenu : Windows, Git Bash, Docker Desktop, pas de WSL (Airflow via Astro CLI en conteneurs).

---

## Avant le jour 1 : poste et dépôt

- [ ] Désactiver l'alias Microsoft Store `python3.exe` (Paramètres, Applications, Paramètres avancés, Alias d'exécution)
- [x] Installer Astro CLI : `winget install Astronomer.Astro` (v1.46.0 ; winget a aussi installé le client Podman CLI 6.1.3, sans machine Podman créée ; Docker Desktop reste le moteur utilisé)
- [x] Récupérer le kit de démarrage dans ce dossier (sparse-checkout ; `core.longpaths` nécessaire sous Windows)
- [x] `git init` + `.gitattributes` (`eol=lf`) pour éviter le CRLF dans les `.sql` et `.sh` (aucun commit fait)
- [ ] `bash verifier_poste.sh` : tout en `OK`
- [x] Créer le dépôt GitHub **public** et pousser le kit (https://github.com/ElieLECAS/Airflow_Snowflakes, vérifié public)
- [x] Créer le compte d'essai Snowflake (Enterprise, région européenne, sans carte bancaire) : région vérifiée `AWS_EU_WEST_3` (Paris) ; édition Enterprise non vérifiée (Admin > Accounts)
- [x] Noter l'identifiant de compte `ORGANISATION-COMPTE` (lu dans l'URL de Snowsight ; gardé hors du dépôt, passé par la variable `SNOWFLAKE_ACCOUNT`)
- [ ] Décider : un compte Snowflake pour le binôme ou un chacun ; un utilisateur de service commun ou un par personne
- [ ] Lire le `README.md` du kit

---

## Jour 1 : Comprendre le pipeline et créer l'entrepôt (Snowflake)

Guide : [10-securite.md](https://github.com/gsoulat/formation-data-IA/blob/main/04-Cloud-Platforms/snowflake/10-securite.md)
Dossiers : `docs/`, `snowflake/`
Compétences visées : C2, C14, C16 (début)

### 1. Lire le schéma du pipeline
- [ ] Repérer les 4 sources/couches (RAW, STAGING, INTERMEDIATE, MARTS) et le rôle de chaque outil
- [ ] Savoir expliquer à l'oral : pourquoi un stage, pourquoi 4 schémas, pourquoi des types larges en RAW

### 2. Explorer janvier et remplir la fiche source
- [x] Télécharger `yellow_tripdata_2025-01.parquet` dans `data/` (hors Git, `*.parquet` est dans le `.gitignore` du kit)
- [x] Mesurer avec DuckDB : 3 475 226 lignes, 20 colonnes, 59 158 238 octets (59,2 Mo, soit 56,4 Mio)
- [x] Lire le dictionnaire de données TLC (PDF) : codes vendeur, tarif, mode de paiement
- [x] Repérer les anomalies : distances nulles, montants négatifs, dates hors janvier 2025, colonnes vides
- [x] `docs/fiche_trajets.md` rédigée d'après le modèle (chiffres **mesurés**, commandes indiquées) : **brouillon à relire ensemble**
- [x] Télécharger `taxi_zone_lookup.csv` dans `data/` (265 lignes, 12 331 octets)
- [x] `docs/fiche_zones.md` rédigée : **brouillon à relire ensemble**
- [ ] Relire les deux fiches, creuser le total_amount, s'approprier les chiffres (chaque membre doit pouvoir les expliquer)

### 3. Suivre le guide sur l'exemple `SALES_DB`
- [ ] Lire les sections 1 et 2 (modèle de droits, privilèges)
- [ ] Reproduire le script de la section 3 sur le compte Snowflake
- [ ] Faire les tests positifs et négatifs de la section 4 (`SALES_ANALYST` refusé hors `ANALYTICS`)
- [ ] Supprimer l'exemple `SALES_*` (exercice de nettoyage)

### 4. Écrire `snowflake/01_entrepot.sql`
Ordre : rôle, rattachement à SYSADMIN, warehouse, base, schémas, droits, utilisateur, rôle donné à l'utilisateur.

> **État : script exécuté dans Snowsight (Run All).** Vérifiés pour l'instant : l'utilisateur `AIRFLOW_SVC` (`DESC USER`) et la connexion par clé. Le reste se coche après `verifications.sql`.
- [x] Rôle des outils (`TRANSFORMER`) créé avec USERADMIN et donné à SYSADMIN (vu : `SHOW GRANTS OF ROLE` → `SYSADMIN` et `AIRFLOW_SVC`)
- [x] Warehouse `NYC_TAXI_WH` : XS, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, suspendu, propriétaire SYSADMIN (vu avec `SHOW WAREHOUSES`)
- [x] Gérer les warehouses créés d'office : `COMPUTE_WH` et `SNOWFLAKE_LEARNING_WH` réglés à 60 s (XS), `SYSTEM$STREAMLIT_NOTEBOOK_WH` déjà à 60 s ; vérifié avec `SHOW WAREHOUSES`. Choix inscrit dans la section 5 de `01_entrepot.sql`.
- [x] Base `NYC_TAXI` + schémas `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS` (noms imposés par le contrat ; vu avec `SHOW SCHEMAS`, propriétaire SYSADMIN)
- [x] Droits minimaux (15 lignes dans `SHOW GRANTS TO ROLE TRANSFORMER`, 5 privilèges distincts, aucun `SELECT`/`INSERT`/`OWNERSHIP`) :
  - [x] warehouse : `USAGE` (15e ligne de la capture, confirmée par la connexion)
  - [x] base : `USAGE`
  - [x] `RAW` : `USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT`
  - [x] `STAGING` : `USAGE, CREATE VIEW, CREATE TABLE` (`codes_tlc.sql` crée 3 **tables**)
  - [x] `INTERMEDIATE` et `MARTS` : `USAGE, CREATE TABLE, CREATE VIEW`
  - [x] Capture des droits (15 lignes) : `docs/captures/jour1_droits_role_transformer.png`
- [x] Utilisateur de service `AIRFLOW_SVC` : `TYPE = SERVICE`, `DEFAULT_ROLE`, `DEFAULT_WAREHOUSE`, `GRANT ROLE ... TO USER` (vu dans `DESC USER`)
- [x] Script **rejouable** : `ALTER WAREHOUSE ... SET AUTO_SUSPEND = 60` et `ALTER USER ... SET ...` après les `CREATE ... IF NOT EXISTS` (2e exécution complète, 128 lignes, sans erreur)
- [x] Exécuter avec **Run All** (pas instruction par instruction)
- [x] Rejouer le script une 2e fois : aucune erreur

### 5. Paire de clés et test de connexion
- [x] Générer la paire dans `~/.ssh/snowflake/` (hors dépôt) : `rsa_key.p8` et `rsa_key.pub` (empreinte `tFiZhc+x…Ddx4Y=`)
- [x] Mettre la clé publique sur une seule ligne, sans `BEGIN` ni `END`, dans l'utilisateur de service (`02_cle_publique.sql`)
- [ ] Binôme avec un seul utilisateur : chacun sa clé (`RSA_PUBLIC_KEY` et `RSA_PUBLIC_KEY_2`), aucune clé privée échangée
- [x] `DESC USER AIRFLOW_SVC` : l'empreinte `RSA_PUBLIC_KEY_FP` est identique à celle calculée avec `openssl` (vérifié ; `RSA_PUBLIC_KEY_2_FP` libre pour le binôme)
- [x] `python -m venv .venv`, `pip install duckdb snowflake-connector-python cryptography`
- [x] Test de connexion Python (`snowflake/test_connexion.py`) : affiche `('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')`

### 6. Vérifications à garder dans le dépôt
- [x] `snowflake/verifications.sql` écrit et exécuté (droits du rôle, `DESC USER`, schémas, warehouses, rattachement à SYSADMIN, tests positifs et refus)
- [ ] Vérifier `SELECT CURRENT_SECONDARY_ROLES();` avant les tests négatifs (sinon ACCOUNTADMIN reste actif en secondaire)
- [x] Test négatif : `CREATE SCHEMA`, `CREATE ROLE` et `ALTER WAREHOUSE … SIZE = 'LARGE'` refusés (3 captures à ranger dans `docs/`). Restent facultatifs : `CREATE DATABASE`, `CREATE WAREHOUSE`, `SELECT` sur `ACCOUNT_USAGE`.

### Résultat à obtenir
- [x] **L'utilisateur de service se connecte depuis le poste avec sa clé** (utilisateur, rôle et warehouse affichés)
- [x] Aucune clé, aucun `.env` dans `git status` (vérifié : `data/` et `.venv/` ignorés, seul `.env.example` contient un exemple de clé)
- [x] Commit + push du jour 1 (`228e4a3`)

---

## Jour 2 : Charger les fichiers dans la couche RAW (Snowflake)

Guide : [11-chargement-stage-copy.md](https://github.com/gsoulat/formation-data-IA/blob/main/04-Cloud-Platforms/snowflake/11-chargement-stage-copy.md)
Dossiers : `snowflake/`, `ingestion/`
Compétences visées : C8, C14

- [ ] Lire `CONTRAT_RAW.md` (noms et colonnes exacts, dont `_source_file` et `_loaded_at`)
- [ ] Suivre le guide sur son exemple
- [x] Créer, **avec le rôle des outils** (jamais ACCOUNTADMIN) : formats de fichier, stage, tables `YELLOW_TRIPDATA` (22 colonnes) et `TAXI_ZONE_LOOKUP` (6) : `snowflake/03_raw.sql`, 5 objets propriétés de `TRANSFORMER`
- [x] Choisir des types larges en RAW (`NUMBER` / `FLOAT` / `TIMESTAMP_NTZ` / `VARCHAR` ; colonnes absentes d'un mois restent vides via `MATCH_BY_COLUMN_NAME`)
- [x] `PUT` de janvier depuis Python (le `PUT` ne marche pas dans Snowsight), fichier à la racine du stage : `UPLOADED`
- [x] `COPY INTO` avec `_source_file` = nom exact du fichier et `_loaded_at` remplis : 3 475 226 lignes, les 10 valeurs de contrôle identiques au Parquet (sommes, dates min/max, NULL, anomalies) ; `USE_LOGICAL_TYPE = TRUE` a bien lu les dates
- [x] Relancer le chargement : aucune ligne en double (2e exécution depuis PowerShell : `PUT` SKIPPED, `COPY` « déjà chargé », toujours 3 475 226 lignes)
- [x] Script Python `ingestion/charger.py` paramétré par le mois (télécharge, dépose, charge) + `requirements.txt` : testé sur les trajets (janvier + rejeu) et sur les zones
- [x] Charger les 265 zones (265 ids distincts de 1 à 265, `N/A` conservés en texte, aucune valeur NULL)
- [ ] (Option Docker) petit `Dockerfile` pour le script, clé montée en volume, jamais dans l'image
- [x] Vérifications du contrat (propriétaires, comptages, décimales conservées via les sommes) : `snowflake/verifications_raw.sql`
- [x] Capture de l'historique de chargement (partie C de `verifications_raw.sql`) : `docs/captures/jour2_historique_chargement.png` (2 fichiers `Loaded`, 3 475 226 et 265 lignes)
- [x] **Résultat : 3 475 226 lignes pour janvier, 265 zones, un 2e chargement n'ajoute rien**

---

## Jour 3 : Premier pipeline Airflow, automatiser le chargement

Guide : [01-airflow3-astro-snowflake.md](https://github.com/gsoulat/formation-data-IA/blob/main/06-Data-Engineering/Airflow/06-Airflow3-Astro/01-airflow3-astro-snowflake.md) sections 1 à 5
Dossier : `airflow/`
Compétences visées : C8, C15, C16

- [ ] Lire les sections 1 à 5 (attention : syntaxe Airflow 3, `from airflow.sdk import dag, task`, `schedule`)
- [x] `astro dev init --force --name nyc-taxi` dans `airflow/` (Runtime `3.3-8`, aucun fichier du kit modifié), `dags/exampledag.py` supprimé
- [x] Créer `airflow/.env` (clé privée sur une seule ligne), **jamais commité** : généré par `airflow/generer_env.py` (1 ligne, 28 retours à la ligne de la clé échappés, ignoré par Git et par `.dockerignore`) ; connexion pas encore testée
- [x] `astro dev start` : l'interface Airflow s'ouvre (5 conteneurs sous Docker, `http://localhost:8080` répond ; l'avertissement « proxy daemon » est sans effet sous Windows)
- [x] Mini DAG `airflow/dags/test_connexion.py` : connexion `snowflake_nyc_taxi` prouvée, le journal de la tâche affiche `('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')`
- [ ] **Rotation de la clé privée** : elle a été affichée en clair dans la conversation (commande `connections get`) ; ancienne clé à retirer (`UNSET RSA_PUBLIC_KEY`) après bascule sur `RSA_PUBLIC_KEY_2`
- [ ] Capture du mini-DAG en succès (facultative) dans `docs/captures/`
- [x] DAG de chargement `airflow/dags/nyc_taxi_pipeline.py` : vérifier que le fichier du mois existe (HEAD), télécharger (taille contrôlée), `PUT`, `COPY INTO` (options du jour 2), contrôle du nombre de lignes en RAW
- [x] Nom du fichier calculé à partir de la **date logique** (`logical_date`), pas de la date du jour ; refus explicite d'un run lancé avec Trigger
- [x] Relances automatiques (`retries=2`, 5 minutes) sur le chargement
- [x] Activer le DAG avec l'interrupteur (`unpause`, jamais Trigger), catchup sur janvier, février, mars 2025 : janvier = `PUT` SKIPPED / « déjà chargé » (rejeu), février et mars chargés pour de vrai
- [ ] Après toute modification du `.env` : `astro dev restart` (à faire après la rotation de la clé)
- [x] **Résultat : 3 exécutions réussies** (en 2 minutes) ; `RAW.YELLOW_TRIPDATA` = 11 198 026 lignes (3 475 226 + 3 577 543 + 4 145 257), identique au kit
- [x] Capture : les 3 exécutions réussies + graphe du DAG (`jour3_grille_trois_executions.png`, `jour3_runs_reussis.png`, `jour3_liste_dags.png`, `jour3_graphe_dag.png`)

---

## Jour 4 : Transformer et contrôler les données avec Airflow

Guide : même guide Airflow, section 6
Dossiers : `airflow/dags/`, `airflow/include/sql/controles/`
Compétences visées : C9, C15

- [ ] Lire la section 6 (fichiers SQL, paramètres, contrôles, groupes de tâches)
- [ ] Lire les fichiers SQL fournis : pour chacun, quelles tables il lit, laquelle il crée
- [x] Déduire l'ordre d'exécution : `00_tables.sql`, `staging/`, `intermediate/`, `marts/` (déduit des `FROM`/`JOIN` ; à relire par toi pour la revue)
- [x] Déclarer les 4 paramètres du DAG : `max_trip_distance_miles=100`, `max_trip_duration_min=180`, `start_month="2025-01-01"`, `end_month="2025-04-01"` + `template_searchpath`
- [x] Une tâche par fichier SQL, regroupées par couche (`split_statements=True` pour les fichiers multi-instructions) : `00_tables`, groupe `staging` (3), `intermediate` (2), `marts` (9) ; le DAG compte 22 tâches avec les contrôles et le chargement
- [x] Brancher le contrôle fourni `controles/raw_mois_charge.sql` (`retries=0`) : `controle_raw_mois_charge`, en succès sur janvier
- [x] Écrire au moins **2 contrôles** de plus : `controles/flagged_taux_rejet.sql` (seuil `max_pct_rejets` = 10 %, les mois rejettent 6,4 / 7,6 / 7,7 %) et `controles/enriched_sans_doublon.sql` ; testés dans Snowflake sur les 3 mois, branchés avec `retries=0`
- [x] **Faire échouer un contrôle exprès** (seuil `max_pct_rejets` à 0) : sur février, `controle_flagged_taux_rejet` en `failed` et les 11 tâches suivantes (`int_trips__enriched`, contrôle des doublons, 9 marts) en `upstream_failed` ; seuil remis à 10, février relancé, 18 tables sur 18 identiques aux valeurs de référence
- [x] Clear des 3 exécutions pour que les nouvelles tâches tournent (22 tâches en succès, environ 55 s par mois)
- [x] Relancer février : le nombre de lignes de chaque table reste identique (18 tables sur 18 identiques ; un seul chargement par fichier dans `COPY_HISTORY`)
- [x] **Résultat : 10 382 378 trajets valides dans `FCT_TRIPS`** (identique au kit ; `FLAGGED` 11 198 026, `MART_ZONE_HOURLY_DEMAND` 11 524, `MART_DATA_QUALITY` 18)
- [x] Capture : un contrôle en échec (`docs/captures/jour4_graphe_echec.png` : run en échec, groupe `intermediate` replié ; une capture avec le groupe déplié, qui montrerait le nœud rouge, peut être ajoutée)

Volumes attendus après les 3 mois :

| Table | Lignes |
|---|---|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 |
| `RAW.TAXI_ZONE_LOOKUP` | 265 |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `MARTS.FCT_TRIPS` | 10 382 378 |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `MARTS.MART_DATA_QUALITY` | 18 |

---

## Jour 5 : Contrôler, documenter et présenter

Dossiers : `docs/`, `README.md`
Compétences visées : C3, C9, C16

- [ ] Requête SQL qui répond à la direction (où et quand la demande est la plus forte, combien rapporte un trajet)
- [ ] `docs/REPONSE.md` (depuis `REPONSE_MODELE.md`) : requête finale, top 10 zones × heures, 3 phrases d'interprétation
- [ ] Compter soi-même les trajets anormaux d'un mois et comparer à `MART_DATA_QUALITY` (une seule raison de rejet par trajet)
- [ ] Mesurer les crédits consommés (`WAREHOUSE_METERING_HISTORY`, rôle ACCOUNTADMIN) + capture
- [ ] Lister les droits du rôle des outils + prouver un accès refusé hors périmètre + capture
- [ ] Capture : historique de chargement Snowflake
- [ ] README complet : description, prérequis, installation pas à pas, schéma, rôle de chaque partie, choix techniques, auteurs
- [ ] Un autre binôme pourrait relancer le dépôt à partir du README
- [ ] Vérifier l'absence de secret dans Git (clés, `.env`, historique inclus)
- [ ] Préparer la démo de 15 minutes
- [ ] Chaque membre sait expliquer l'ensemble

Bonus :
- [ ] Notification Discord à la fin de chaque exécution (webhook = secret)
- [ ] Déploiement sur Astro (Airflow hébergé), sans secret dans l'image

---

## Livrables

- [ ] Dépôt GitHub public : `README.md`, `snowflake/`, `ingestion/`, `airflow/`, `docs/`, `.gitignore`
- [ ] Captures dans `docs/` :
  - [ ] 3 exécutions réussies
  - [ ] graphe du DAG
  - [ ] un contrôle en échec
  - [ ] historique de chargement Snowflake
  - [ ] droits du rôle des outils
  - [ ] suivi des crédits
- [ ] `docs/REPONSE.md`
- [ ] `docs/fiche_trajets.md`

## Déroulé de la démonstration (15 min + 10 min de questions)

- [ ] 1. Droits du rôle des outils + un accès refusé hors périmètre
- [ ] 2. Les 3 exécutions Airflow + le graphe du DAG
- [ ] 3. Relancer un mois : aucun doublon
- [ ] 4. Faire échouer un contrôle : la suite ne s'exécute pas
- [ ] 5. Requête qui répond à la question centrale
- [ ] 6. Crédits consommés

Questions à préparer : pourquoi ces droits et pas davantage, pourquoi un stage, pourquoi la date logique, pourquoi cet ordre de tâches, que se passerait-il avec un quatrième mois.
