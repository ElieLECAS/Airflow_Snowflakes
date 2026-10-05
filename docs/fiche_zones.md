# Fiche source : liste des zones de taxi (Taxi Zone Lookup)

Brouillon rempli à partir de `data/taxi_zone_lookup.csv`. Tous les chiffres ont été mesurés avec DuckDB.

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Taxi Zone Lookup Table (liste des zones de taxi de New York) |
| Producteur des données | NYC Taxi & Limousine Commission (TLC) |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` |
| Accès (public, authentifié) | Public, sans authentification |
| Format du fichier | CSV avec en-tête, textes entre guillemets |
| Fréquence de publication | Fichier de référence, chargé une seule fois (hors Airflow) d'après le brief |
| Délai entre la période couverte et la publication | Sans objet (table de référence, pas une période) |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `taxi_zone_lookup.csv` | 12 331 octets | 265 (hors en-tête) | 4 | DuckDB `SELECT COUNT(*) FROM read_csv_auto('taxi_zone_lookup.csv')` et `DESCRIBE` |

## Colonnes

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `LocationID` | BIGINT | Identifiant de la zone, unique (1 à 265) | 4 |
| `Borough` | VARCHAR | Arrondissement | Manhattan |
| `Zone` | VARCHAR | Nom de la zone | Alphabet City |
| `service_zone` | VARCHAR | Zone de service des taxis | Yellow Zone |

## Codes

| Colonne | Valeur | Signification |
|---|---|---|
| `Borough` | Queens (69), Manhattan (69), Brooklyn (61), Bronx (43), Staten Island (20) | Les cinq arrondissements |
| `Borough` | EWR (1), Unknown (1), N/A (1) | Aéroport de Newark, zone inconnue, hors de New York |
| `service_zone` | Boro Zone (205), Yellow Zone (55), Airports (2), EWR (1), N/A (2) | Zones de service, d'après les valeurs observées. Le dictionnaire des zones n'a pas été lu. |

## Ce qui a surpris

- **Les `N/A` sont du texte, pas des valeurs vides.** Les zones 264 (`Unknown`) et 265 (`Outside of NYC`) portent la chaîne `N/A`. Aucune valeur n'est réellement NULL : à garder en tête pour les options du format de fichier au jour 2.
- **Le nom d'une zone n'est pas unique** : `Corona` apparaît 2 fois et `Governor's Island/Ellis Island/Liberty Island` 3 fois. Seul `LocationID` identifie une zone.
- **Aucun identifiant du fichier de janvier n'est absent de la liste** : toutes les zones de départ et d'arrivée des trajets sont présentes.
