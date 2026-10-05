"""Test de connexion de l'utilisateur de service par paire de clés (jour 1).

Valide d'un coup l'identifiant de compte, la clé, le rôle donné à l'utilisateur
et le droit sur le warehouse. Résultat attendu :

    ('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')

Configuration par variables d'environnement, rien n'est écrit dans le code :
    SNOWFLAKE_ACCOUNT           obligatoire, au format ORGANISATION-COMPTE
    SNOWFLAKE_USER              défaut : AIRFLOW_SVC
    SNOWFLAKE_ROLE              défaut : TRANSFORMER
    SNOWFLAKE_WAREHOUSE         défaut : NYC_TAXI_WH
    SNOWFLAKE_PRIVATE_KEY_PATH  défaut : ~/.ssh/snowflake/rsa_key.p8
"""
import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization

account = os.environ.get("SNOWFLAKE_ACCOUNT")
if not account:
    sys.exit("Définir SNOWFLAKE_ACCOUNT au format ORGANISATION-COMPTE (ni l'URL, ni le nom d'utilisateur).")

key_path = Path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "~/.ssh/snowflake/rsa_key.p8")).expanduser()
private_key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)

# Le connecteur attend la clé privée en DER (PKCS8), sans chiffrement.
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

conn = snowflake.connector.connect(
    account=account,
    user=os.environ.get("SNOWFLAKE_USER", "AIRFLOW_SVC"),
    private_key=private_key_der,
    role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NYC_TAXI_WH"),
)
try:
    print(conn.cursor().execute("select current_user(), current_role(), current_warehouse()").fetchone())
finally:
    conn.close()
