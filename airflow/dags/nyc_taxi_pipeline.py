"""Pipeline NYC Yellow Taxi : chargement mensuel des trajets dans la couche RAW (jour 3).

Un run égale un mois. Le fichier traité se déduit de la date logique du run, jamais de la date du jour :
relancer le run de janvier en octobre recharge bien le fichier de janvier.

    nom_du_fichier -> verifier_disponibilite -> telecharger_et_deposer -> copier_dans_la_table

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
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task
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

    copier_dans_la_table(telecharger_et_deposer(verifier_disponibilite(nom_du_fichier())))


nyc_taxi_pipeline()
