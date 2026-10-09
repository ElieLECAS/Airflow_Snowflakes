"""Pipeline NYC Yellow Taxi : chargement mensuel dans RAW (jour 3), puis transformations et contrôles (jour 4).

Un run égale un mois. Le fichier traité se déduit de la date logique du run, jamais de la date du jour :
relancer le run de janvier en octobre recharge bien le fichier de janvier.

    nom_du_fichier -> verifier_disponibilite -> telecharger_et_deposer -> copier_dans_la_table
        -> controle_raw_mois_charge -> creer_tables -> groupe staging (3 tâches en parallèle)
        -> groupe intermediate : int_trips__flagged -> contrôle du taux de rejet
           -> int_trips__enriched -> contrôle des doublons
        -> groupe marts : 5 dimensions + fct_trips en parallèle, puis 3 tables d'analyse

Une tâche SQL égale un fichier de include/sql/. Les fichiers contiennent {{ ds }}, {{ logical_date }} et
{{ params.xxx }}, remplacés par Airflow avant l'exécution.

Rejouable : Snowflake retient les fichiers déjà copiés dans la table, relancer un mois n'ajoute aucune ligne.
La connexion `snowflake_nyc_taxi` vient de airflow/.env (variable AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI) :
aucune clé dans ce fichier, ni dans l'image Docker.

À activer avec l'interrupteur, jamais avec Trigger : un run lancé à la main n'a pas de date logique.
"""
import logging
from contextlib import closing
from pathlib import Path

import pendulum
import requests
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import TaskGroup, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

CONN_ID = "snowflake_nyc_taxi"
URL_TRAJETS = "https://d37ci6vzurychx.cloudfront.net/trip-data/{fichier}"

SCHEMA = "NYC_TAXI.RAW"
STAGE = f"@{SCHEMA}.TLC_STAGE"
TABLE = f"{SCHEMA}.YELLOW_TRIPDATA"
FORMAT_FICHIER = f"{SCHEMA}.PARQUET_FF"

log = logging.getLogger(__name__)


def executer(sql: str, parametres: tuple | None = None) -> list:
    """Exécute une instruction sur Snowflake avec la connexion Airflow et renvoie ses lignes."""
    with closing(SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn()) as conn:
        with conn.cursor() as curseur:
            curseur.execute(sql, parametres)
            return curseur.fetchall()


@dag(
    dag_id="nyc_taxi_pipeline",
    description="Chargement mensuel des trajets TLC dans NYC_TAXI.RAW",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),    # janvier, février, mars 2025
    catchup=True,                                        # un run par mois, rattrapés à l'activation
    max_active_runs=1,                                   # un mois à la fois, dans l'ordre
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["nyc-taxi", "raw"],
    template_searchpath="/usr/local/airflow/include/sql",    # chemin DANS le conteneur, pour trouver les fichiers .sql
    params={                                                 # lus par les fichiers SQL : {{ params.xxx }}
        "max_trip_distance_miles": 100,                      # int_trips__flagged.sql
        "max_trip_duration_min": 180,                        # int_trips__flagged.sql
        "start_month": "2025-01-01",                         # dim_date.sql
        "end_month": "2025-04-01",                           # dim_date.sql
        "max_pct_rejets": 10,                                # controles/flagged_taux_rejet.sql (% de trajets écartés)
    },
)
def nyc_taxi_pipeline():

    @task
    def nom_du_fichier() -> str:
        date_logique = get_current_context()["logical_date"]
        if date_logique is None:
            raise AirflowFailException(
                "Ce run n'a pas de date logique (lancé avec Trigger ?). "
                "Activer le DAG avec l'interrupteur : chaque mois est alors traité à sa date."
            )
        return f"yellow_tripdata_{date_logique.strftime('%Y-%m')}.parquet"

    @task
    def verifier_disponibilite(fichier: str) -> str:
        url = URL_TRAJETS.format(fichier=fichier)
        reponse = requests.head(url, timeout=30)
        if reponse.status_code in (403, 404):      # le serveur répond 403 pour un fichier absent
            raise RuntimeError(f"{fichier} n'est pas encore publié ({reponse.status_code}) : {url}")
        reponse.raise_for_status()
        log.info("%s est publié (%s octets)", fichier, reponse.headers.get("Content-Length"))
        return url

    @task
    def telecharger_et_deposer(url: str) -> str:
        # Téléchargement et PUT dans la même tâche : deux tâches pourraient tourner sur deux machines
        # qui ne partagent pas /tmp.
        fichier = url.rsplit("/", 1)[-1]
        destination = Path("/tmp") / fichier
        try:
            with requests.get(url, stream=True, timeout=120) as reponse:
                reponse.raise_for_status()
                attendu = int(reponse.headers["Content-Length"])
                with destination.open("wb") as f:
                    for morceau in reponse.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(morceau)
            recu = destination.stat().st_size
            if recu != attendu:
                raise RuntimeError(f"Téléchargement incomplet : {recu} octets reçus sur {attendu}")
            log.info("Téléchargé : %s (%s octets)", fichier, recu)

            # AUTO_COMPRESS=FALSE : le nom du fichier reste intact (il devient _source_file).
            # OVERWRITE=FALSE : un fichier déjà déposé n'est pas renvoyé (statut SKIPPED).
            statut = executer(
                f"PUT 'file://{destination.as_posix()}' {STAGE} AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
            )[0][6]
            log.info("PUT %s -> %s", fichier, statut)
        finally:
            destination.unlink(missing_ok=True)      # le disque du conteneur est limité
        return fichier

    @task
    def copier_dans_la_table(fichier: str) -> int:
        # Mêmes options qu'au jour 2 (ingestion/charger.py). Pas de FORCE : un fichier déjà chargé
        # n'est pas rechargé, donc relancer le run n'ajoute aucune ligne.
        lignes = executer(f"""
            COPY INTO {TABLE}
            FROM {STAGE}
            FILES = ('{fichier}')
            FILE_FORMAT = (FORMAT_NAME = {FORMAT_FICHIER})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)
            ON_ERROR = ABORT_STATEMENT
        """)
        # Une ligne par fichier chargé (colonne 4 : lignes chargées). Si le fichier a déjà été chargé,
        # Snowflake répond « 0 files processed » sans détail : aucune ligne LOADED.
        chargees = sum(int(l[3]) for l in lignes if len(l) > 3 and l[1] == "LOADED")
        if chargees:
            log.info("COPY %s : %s lignes chargées", fichier, chargees)
        else:
            log.info("COPY %s : fichier déjà chargé, rien à faire", fichier)

        dans_raw = executer(f"SELECT COUNT(*) FROM {TABLE} WHERE _source_file = %s", (fichier,))[0][0]
        log.info("RAW contient %s lignes pour %s", dans_raw, fichier)
        if dans_raw == 0:
            raise AirflowFailException(f"Aucune ligne de {fichier} dans RAW après le chargement.")
        return chargees

    chargement = copier_dans_la_table(telecharger_et_deposer(verifier_disponibilite(nom_du_fichier())))

    # Contrôle fourni : le fichier du mois est bien dans RAW. Pas de relance (retries=0) : relire les
    # mêmes données ne changerait pas le résultat, la tâche attendrait pour rien avant d'échouer.
    controle_raw_mois_charge = SQLCheckOperator(
        task_id="controle_raw_mois_charge",
        conn_id=CONN_ID,
        sql="controles/raw_mois_charge.sql",
        retries=0,
    )

    # Crée les trois tables alimentées mois par mois (CREATE TABLE IF NOT EXISTS : rejouable).
    creer_tables = SQLExecuteQueryOperator(
        task_id="creer_tables",
        conn_id=CONN_ID,
        sql="00_tables.sql",
        split_statements=True,                               # le fichier contient plusieurs instructions
    )

    # Les trois fichiers de staging ne dépendent pas les uns des autres : ils tournent en parallèle.
    with TaskGroup("staging") as staging:
        for fichier in ("codes_tlc", "stg_tlc__taxi_zones", "stg_tlc__yellow_trips"):
            SQLExecuteQueryOperator(
                task_id=fichier,
                conn_id=CONN_ID,
                sql=f"staging/{fichier}.sql",
                split_statements=True,
            )

    # int_trips__flagged marque chaque trajet du mois (valide ou raison du rejet), puis int_trips__enriched
    # ne garde que les valides, dédoublonnés. Les deux fichiers font DELETE du mois puis INSERT.
    # Chaque contrôle se place juste après ce qu'il vérifie et avant ce qui utilise le résultat :
    # une donnée mauvaise n'est pas propagée à la couche suivante.
    with TaskGroup("intermediate") as intermediate:
        flagged = SQLExecuteQueryOperator(
            task_id="int_trips__flagged",
            conn_id=CONN_ID,
            sql="intermediate/int_trips__flagged.sql",
            split_statements=True,
        )
        controle_taux_rejet = SQLCheckOperator(            # trop de trajets écartés ?
            task_id="controle_flagged_taux_rejet",
            conn_id=CONN_ID,
            sql="controles/flagged_taux_rejet.sql",
            retries=0,
        )
        enriched = SQLExecuteQueryOperator(
            task_id="int_trips__enriched",
            conn_id=CONN_ID,
            sql="intermediate/int_trips__enriched.sql",
            split_statements=True,
        )
        controle_doublons = SQLCheckOperator(              # trajets en double ?
            task_id="controle_enriched_sans_doublon",
            conn_id=CONN_ID,
            sql="controles/enriched_sans_doublon.sql",
            retries=0,
        )
        flagged >> controle_taux_rejet >> enriched >> controle_doublons

    # Les cinq dimensions et la table de faits ne dépendent que de staging et d'intermediate : elles tournent
    # en parallèle. Les trois tables d'analyse lisent la table de faits et des dimensions : elles passent après.
    with TaskGroup("marts") as marts:
        dimensions_et_faits = [
            SQLExecuteQueryOperator(task_id=nom, conn_id=CONN_ID, sql=f"marts/{nom}.sql", split_statements=True)
            for nom in ("dim_date", "dim_payment_type", "dim_rate_code", "dim_vendor", "dim_zone", "fct_trips")
        ]
        analyses = [
            SQLExecuteQueryOperator(task_id=nom, conn_id=CONN_ID, sql=f"marts/{nom}.sql", split_statements=True)
            for nom in ("mart_daily_revenue", "mart_zone_hourly_demand", "mart_data_quality")
        ]
        for tache in dimensions_et_faits:        # une boucle : Python ne sait pas relier deux listes avec >>
            tache >> analyses

    chargement >> controle_raw_mois_charge >> creer_tables >> staging >> intermediate >> marts


nyc_taxi_pipeline()
