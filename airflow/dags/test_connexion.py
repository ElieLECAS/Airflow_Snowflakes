"""Prouve qu'Airflow se connecte à Snowflake avec la connexion `snowflake_nyc_taxi` (jour 3).

Un DAG d'une seule tâche, sans planification : on le lance à la main. Le log de la tâche doit afficher
('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH'). La clé privée vit dans airflow/.env, jamais dans ce fichier.
"""
import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

CONN_ID = "snowflake_nyc_taxi"


@dag(
    dag_id="test_connexion",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)
def test_connexion():
    @task
    def qui_suis_je():
        ligne = SnowflakeHook(snowflake_conn_id=CONN_ID).get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
        )
        print(f"CONNEXION_OK {ligne}")

    qui_suis_je()


test_connexion()
