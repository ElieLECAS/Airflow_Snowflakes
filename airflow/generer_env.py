"""Construit airflow/.env : la connexion Airflow `snowflake_nyc_taxi`, authentifiée par clé privée.

Usage :
    SNOWFLAKE_ACCOUNT=ORGANISATION-COMPTE python airflow/generer_env.py

La clé privée est lue sur le poste, mise sur une seule ligne (les retours à la ligne deviennent
les deux caractères \\n) et écrite dans airflow/.env. Elle n'est jamais affichée. Le fichier .env
est ignoré par Git (.gitignore) et par Docker (.dockerignore) : la clé n'entre ni dans le dépôt,
ni dans l'image.

Variables d'environnement :
    SNOWFLAKE_ACCOUNT           obligatoire, au format ORGANISATION-COMPTE
    SNOWFLAKE_USER              défaut : AIRFLOW_SVC
    SNOWFLAKE_ROLE              défaut : TRANSFORMER
    SNOWFLAKE_WAREHOUSE         défaut : NYC_TAXI_WH
    SNOWFLAKE_PRIVATE_KEY_PATH  défaut : ~/.ssh/snowflake/rsa_key.p8

Option --force : remplace un .env déjà rempli (sans elle, le script refuse de l'écraser).
"""
import json
import os
import sys
from pathlib import Path

CONN_ID = "snowflake_nyc_taxi"
SORTIE = Path(__file__).resolve().parent / ".env"
ENTETE_CLE = "-----BEGIN PRIVATE KEY-----"
PIED_CLE = "-----END PRIVATE KEY-----"


def main() -> None:
    compte = os.environ.get("SNOWFLAKE_ACCOUNT")
    if not compte:
        sys.exit("Définir SNOWFLAKE_ACCOUNT au format ORGANISATION-COMPTE (ni l'URL, ni le nom d'utilisateur).")
    if SORTIE.exists() and SORTIE.stat().st_size > 0 and "--force" not in sys.argv:
        sys.exit(f"{SORTIE} contient déjà quelque chose : relancer avec --force pour le remplacer.")

    chemin_cle = Path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "~/.ssh/snowflake/rsa_key.p8")).expanduser()
    cle = chemin_cle.read_text(encoding="utf-8").strip()
    if not (cle.startswith(ENTETE_CLE) and cle.endswith(PIED_CLE)):
        sys.exit(f"{chemin_cle} n'est pas une clé privée PKCS8 non chiffrée (attendu : {ENTETE_CLE}).")

    connexion = {
        "conn_type": "snowflake",
        "login": os.environ.get("SNOWFLAKE_USER", "AIRFLOW_SVC"),
        "extra": {
            "account": compte,
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "NYC_TAXI_WH"),
            "database": "NYC_TAXI",
            "role": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
            # json.dumps écrit les retours à la ligne de la clé sous la forme \n, sur une seule ligne.
            "private_key_content": cle + "\n",
        },
    }
    ligne = f"AIRFLOW_CONN_{CONN_ID.upper()}='{json.dumps(connexion, separators=(',', ':'))}'\n"
    SORTIE.write_text(ligne, encoding="utf-8", newline="\n")
    try:
        os.chmod(SORTIE, 0o600)                  # sans effet sous Windows
    except OSError:
        pass

    print(f"Écrit : {SORTIE.name} ({len(ligne)} caractères)")
    print(f"Connexion Airflow : {CONN_ID}  |  compte {compte}  |  utilisateur {connexion['login']}  |  rôle {connexion['extra']['role']}")
    print("La clé privée n'est pas affichée. Relancer `astro dev restart` après toute modification de .env.")


if __name__ == "__main__":
    main()
