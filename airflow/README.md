# Projet Airflow

Projet Astro (Astro Runtime 3.3, Airflow 3) qui fait tourner le DAG `nyc_taxi_pipeline`. L'installation complète est décrite dans le README à la racine du dépôt.

```
dags/nyc_taxi_pipeline.py   le DAG : chargement, transformations, contrôles, notification
dags/test_connexion.py      petit DAG qui vérifie la connexion Snowflake
include/sql/                un fichier SQL par tâche (staging, intermediate, marts, controles)
generer_env.py              construit .env à partir de la clé privée du poste
.env.example                format du .env (le vrai .env n'est jamais versionné)
```

Commandes utiles, depuis ce dossier :

```bash
astro dev start      # démarre Airflow sur http://localhost:8080
astro dev restart    # après un changement de .env ou de requirements.txt
astro dev stop       # arrête les conteneurs
```

Un DAG nouveau n'apparaît parfois qu'après `astro dev run dags reserialize`.
