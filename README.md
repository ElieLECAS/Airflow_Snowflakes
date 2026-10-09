# Pipeline NYC Yellow Taxi

Pipeline de données pour Hudson Cab Partners : les trajets des taxis jaunes de New York (janvier à mars 2025) sont téléchargés, chargés dans Snowflake, nettoyés, contrôlés, puis mis en forme pour répondre à une question de la direction : où et quand la demande est-elle la plus forte, et combien rapporte un trajet ?

Airflow lance le tout, un mois à la fois. Snowflake stocke et transforme.

![Schéma du pipeline](docs/architecture.png)

## Ce que fait le pipeline

Pour chaque mois, le DAG `nyc_taxi_pipeline` enchaîne :

1. le téléchargement du fichier Parquet du mois et son dépôt sur un stage Snowflake ;
2. le chargement dans `RAW.YELLOW_TRIPDATA` (`COPY INTO`), sans modifier les données ;
3. le staging : renommage et typage des colonnes, tables de codes ;
4. l'intermediate : chaque trajet est marqué valide ou rejeté (avec la première raison du rejet), puis les trajets valides sont enrichis et dédoublonnés ;
5. les marts : cinq dimensions, la table de faits `FCT_TRIPS` et trois tables d'analyse.

Trois contrôles bloquent la suite s'ils échouent : le mois est bien dans RAW, le taux de rejet reste sous 10 %, il n'y a aucun doublon dans les trajets enrichis. Une dernière tâche envoie le résultat du run sur Discord (facultatif).

Un run correspond à un mois. Le fichier traité se déduit de la date logique du run, pas de la date du jour : rejouer janvier en octobre recharge bien janvier.

## Contenu du dépôt

```
snowflake/      scripts SQL : entrepôt et droits, clé publique, couche RAW, vérifications, requête finale
ingestion/      charger.py : chargement d'un mois à la main (jour 2)
airflow/        projet Astro : DAG, fichiers SQL, génération du .env
docs/           réponse à la direction, fiches des sources, anomalies, captures
```

Les fichiers SQL des transformations sont dans `airflow/include/sql/` (un fichier par tâche). Ceux de `snowflake/` se lancent à la main dans Snowsight.

## Ce qu'il faut avant de commencer

- un compte Snowflake (l'essai gratuit suffit) et un accès `ACCOUNTADMIN` pour la première installation ;
- Python 3.11 ou plus ;
- Git pour Windows, qui fournit Git Bash et OpenSSL ;
- Docker Desktop, lancé ;
- l'Astro CLI : `winget install -e --id Astronomer.Astro`.

Tout a été fait sous Windows 11 avec Git Bash et PowerShell, sans WSL. Les commandes ci-dessous sont pour Git Bash.

## Installation

### 1. La clé de l'utilisateur de service

Les outils (Airflow, scripts Python) se connectent à Snowflake avec une paire de clés, pas avec un mot de passe. On la crée sur son poste :

```bash
mkdir -p ~/.ssh/snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out ~/.ssh/snowflake/rsa_key.p8
openssl rsa -in ~/.ssh/snowflake/rsa_key.p8 -pubout -out ~/.ssh/snowflake/rsa_key.pub
```

La clé privée (`rsa_key.p8`) ne quitte jamais le poste. Seule la clé publique va dans Snowflake.

### 2. L'entrepôt et les droits

Dans Snowsight, ouvrir une feuille et lancer dans cet ordre (Run All) :

1. `snowflake/01_entrepot.sql` crée le warehouse `NYC_TAXI_WH` (XS, arrêt après 60 s), la base `NYC_TAXI` avec ses quatre schémas, le rôle `TRANSFORMER` et l'utilisateur `AIRFLOW_SVC`.
2. `snowflake/02_cle_publique.sql` enregistre la clé publique. Il faut y remplacer la valeur de `RSA_PUBLIC_KEY` par la sienne : `grep -v "BEGIN\|END" ~/.ssh/snowflake/rsa_key.pub | tr -d '\n'`.
3. `snowflake/03_raw.sql`, avec le rôle `TRANSFORMER` (le script le sélectionne), crée les formats de fichier, le stage et les deux tables de RAW.

`TRANSFORMER` n'a que le nécessaire : `USAGE` sur la base, les schémas et le warehouse, et le droit de créer des tables, vues, stages et formats de fichier. Il ne peut ni créer de schéma, ni modifier le warehouse, ni lire la facturation. `snowflake/verifications.sql` le vérifie.

### 3. Tester la connexion

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r ingestion/requirements.txt
export SNOWFLAKE_ACCOUNT=ORGANISATION-COMPTE
python snowflake/test_connexion.py
```

`ORGANISATION-COMPTE` est l'identifiant du compte (dans Snowsight : le menu du compte, puis « Copy account identifier »). La réponse attendue est `('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')`.

### 4. La table des zones

La liste des 265 zones est chargée une seule fois, hors Airflow :

```bash
python ingestion/charger.py zones
```

### 5. Airflow

```bash
cd airflow
python generer_env.py          # écrit airflow/.env avec la connexion Snowflake
astro dev start
```

`generer_env.py` lit la clé privée sur le poste et construit la connexion dans `airflow/.env`. Ce fichier est ignoré par Git et par Docker : aucune clé ne se retrouve dans le dépôt ni dans l'image. Il se sert de la variable `SNOWFLAKE_ACCOUNT` déjà définie ; si le terminal est neuf, la redéfinir. `airflow/.env.example` montre le format.

L'interface est sur http://localhost:8080. Le démarrage prend quelques minutes la première fois.

## Lancer le pipeline

1. Dans l'interface, activer le DAG `nyc_taxi_pipeline` avec son interrupteur. Ne pas utiliser « Trigger » : un run lancé à la main n'a pas de date logique et le DAG refuse de démarrer.
2. Les trois runs (janvier, février, mars) se lancent l'un après l'autre. Chacun dure environ une minute.
3. Pour rejouer un mois sans doublon : ouvrir le run et utiliser « Clear ». Snowflake se souvient des fichiers déjà copiés, rien n'est rechargé.

Résultats attendus après les trois mois :

| Table | Lignes |
|---|---|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 |
| `RAW.TAXI_ZONE_LOOKUP` | 265 |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `MARTS.FCT_TRIPS` | 10 382 378 |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `MARTS.MART_DATA_QUALITY` | 18 |

La requête qui répond à la direction est dans `snowflake/requete_direction.sql`, le résultat et son commentaire dans `docs/REPONSE.md`.

### Notification Discord

Pour recevoir un message à la fin de chaque run, créer un webhook dans un salon Discord et ajouter une ligne à `airflow/.env` :

```
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

Puis `astro dev restart`. Sans cette variable, la tâche ne fait rien. Le webhook est un secret : il ne va pas dans Git.

## Choix techniques

- **Clés plutôt que mot de passe.** L'utilisateur de service est de type `SERVICE`, il ne peut pas se connecter avec un mot de passe. Il accepte deux clés publiques, une par personne du binôme, sans échange de clé privée.
- **Un rôle dédié aux outils.** Aucun outil n'utilise `ACCOUNTADMIN`. `TRANSFORMER` est rattaché à `SYSADMIN`, qui voit donc tout ce qu'il crée.
- **Types larges en RAW.** `NUMBER`, `FLOAT`, `TIMESTAMP_NTZ`, `VARCHAR` : un même champ change de type d'un mois à l'autre dans les fichiers TLC, et RAW ne doit rien refuser.
- **`COPY INTO` sans `FORCE`.** Snowflake retient 64 jours les fichiers déjà chargés. C'est ce qui rend le rechargement d'un mois sans effet.
- **Un mois = un `DELETE` puis un `INSERT`** dans les tables intermédiaires et la table de faits, pour que rejouer un mois remplace ses lignes sans toucher aux autres.
- **Téléchargement et dépôt sur le stage dans la même tâche.** Deux tâches pourraient tourner sur deux machines qui ne partagent pas `/tmp`.
- **Contrôles sans relance** (`retries=0`) : relire les mêmes données ne changerait pas le résultat.
- **Warehouse XS, arrêt après 60 secondes.** Le projet entier a consommé un peu moins d'un crédit.

## Ajouter un mois

Deux endroits à changer : `end_date` du DAG dans `airflow/dags/nyc_taxi_pipeline.py`, et le paramètre `end_month` du même fichier (utilisé par `dim_date`). Sans le second, les trajets du nouveau mois n'auraient pas de date dans la dimension.

## Limites

- Trois mois d'hiver seulement : pas de saisonnalité.
- 7,3 % des trajets sont écartés par les règles de qualité (815 648 sur 11 198 026). Le détail est dans `docs/anomalies_janvier.md`.
- Le DAG tourne en local : les notifications ne partent que si Docker est lancé.
- Les fichiers TLC sont publiés avec environ deux mois de retard. Un mois pas encore publié fait échouer la tâche `verifier_disponibilite` (le serveur répond 403).

## Documentation

- `docs/REPONSE.md` : réponse à la direction
- `docs/anomalies_janvier.md` : trajets rejetés en janvier, comptés à la main
- `docs/fiche_trajets.md`, `docs/fiche_zones.md` : fiches des deux sources
- `docs/captures/` : captures des runs, des droits, des crédits et des contrôles

## Auteur

Elie Lecas
