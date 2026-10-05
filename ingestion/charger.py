"""Charge un fichier TLC dans la couche RAW de Snowflake (jour 2).

Usage :
    python ingestion/charger.py trajets 2025-01    # les trajets d'un mois
    python ingestion/charger.py zones              # la liste des 265 zones (une seule fois)

Le script télécharge le fichier, le dépose sur le stage (PUT), puis le copie dans
la table (COPY INTO). Rejouable : Snowflake retient pendant 64 jours les fichiers
déjà copiés dans une table, relancer le même mois ne crée donc aucune ligne en double.

Configuration par variables d'environnement, aucun secret dans le code :
    SNOWFLAKE_ACCOUNT           obligatoire, au format ORGANISATION-COMPTE
    SNOWFLAKE_USER              défaut : AIRFLOW_SVC
    SNOWFLAKE_ROLE              défaut : TRANSFORMER
    SNOWFLAKE_WAREHOUSE         défaut : NYC_TAXI_WH
    SNOWFLAKE_PRIVATE_KEY_PATH  défaut : ~/.ssh/snowflake/rsa_key.p8
"""
import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

URL_TRAJETS = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mois}.parquet"
URL_ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

SCHEMA = "NYC_TAXI.RAW"
STAGE = f"@{SCHEMA}.TLC_STAGE"


def telecharger(url: str, dossier: Path) -> Path:
    """Télécharge le fichier par morceaux (60 Mo, inutile de le garder en mémoire)."""
    destination = dossier / url.rsplit("/", 1)[-1]
    with requests.get(url, stream=True, timeout=120) as reponse:
        if reponse.status_code in (403, 404):
            sys.exit(f"Fichier introuvable ({reponse.status_code}) : {url}\nLe mois est-il déjà publié ?")
        reponse.raise_for_status()
        with destination.open("wb") as f:
            for morceau in reponse.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(morceau)
    return destination


def connecter():
    """Connexion par paire de clés : la clé privée reste sur le poste, jamais dans le code."""
    if "SNOWFLAKE_ACCOUNT" not in os.environ:
        sys.exit("Définir SNOWFLAKE_ACCOUNT au format ORGANISATION-COMPTE.")
    chemin = Path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "~/.ssh/snowflake/rsa_key.p8")).expanduser()
    cle = serialization.load_pem_private_key(chemin.read_bytes(), password=None)
    cle_der = cle.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("SNOWFLAKE_USER", "AIRFLOW_SVC"),
        private_key=cle_der,
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NYC_TAXI_WH"),
    )


def charger(conn, fichier: Path, table: str, format_fichier: str) -> int:
    """Dépose le fichier sur le stage puis le copie dans la table.

    Renvoie le nombre de lignes chargées : 0 si Snowflake a déjà chargé ce fichier.
    """
    with conn.cursor() as cur:
        # AUTO_COMPRESS=FALSE : le nom du fichier reste intact (il devient _source_file).
        # OVERWRITE=FALSE : un fichier déjà déposé n'est pas renvoyé (statut SKIPPED).
        # Guillemets simples : le chemin peut contenir des espaces.
        cur.execute(f"PUT 'file://{fichier.resolve().as_posix()}' {STAGE} AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
        print(f"PUT      : {fichier.name} -> {cur.fetchone()[6]}")

        cur.execute(f"""
            COPY INTO {SCHEMA}.{table}
            FROM {STAGE}
            FILES = ('{fichier.name}')
            FILE_FORMAT = (FORMAT_NAME = {SCHEMA}.{format_fichier})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)
            ON_ERROR = ABORT_STATEMENT
        """)
        # Une ligne par fichier chargé (colonne 4 : lignes chargées). Si le fichier a déjà
        # été chargé, Snowflake répond « 0 files processed » sans détail : aucune ligne LOADED.
        lignes = [l for l in cur.fetchall() if len(l) > 3 and l[1] == "LOADED"]
        charge = sum(int(l[3]) for l in lignes)
        print(f"COPY     : {charge} lignes chargées" if lignes else "COPY     : fichier déjà chargé, rien à faire")

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table} WHERE _source_file = %s", (fichier.name,))
        print(f"RAW      : {cur.fetchone()[0]} lignes pour {fichier.name}")
    return charge


def main():
    parseur = argparse.ArgumentParser(description="Charge un fichier TLC dans la couche RAW.")
    sous = parseur.add_subparsers(dest="source", required=True)
    trajets = sous.add_parser("trajets", help="les trajets des taxis jaunes d'un mois")
    trajets.add_argument("mois", help="mois au format AAAA-MM, par exemple 2025-01")
    sous.add_parser("zones", help="la liste des zones de taxi (une seule fois)")
    args = parseur.parse_args()

    if args.source == "trajets":
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.mois):
            sys.exit(f"Mois invalide : {args.mois!r} (attendu : AAAA-MM)")
        url, table, format_fichier = URL_TRAJETS.format(mois=args.mois), "YELLOW_TRIPDATA", "PARQUET_FF"
    else:
        url, table, format_fichier = URL_ZONES, "TAXI_ZONE_LOOKUP", "CSV_FF"

    # On se connecte d'abord : une clé ou un compte erroné échoue avant de télécharger 60 Mo.
    conn = connecter()
    try:
        # Le fichier téléchargé vit dans un dossier temporaire, supprimé à la fin.
        with tempfile.TemporaryDirectory() as dossier:
            print(f"Téléchargement de {url}")
            fichier = telecharger(url, Path(dossier))
            print(f"Téléchargé : {fichier.name} ({fichier.stat().st_size} octets)")
            charger(conn, fichier, table, format_fichier)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
