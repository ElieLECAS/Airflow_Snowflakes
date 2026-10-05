# Fiche source : trajets des taxis jaunes (Yellow Taxi Trip Records)

Brouillon rempli à partir du fichier de janvier 2025. Tous les chiffres ont été mesurés avec DuckDB sur `data/yellow_tripdata_2025-01.parquet`, sauf mention contraire.

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Yellow Taxi Trip Records (trajets des taxis jaunes de New York) |
| Producteur des données | NYC Taxi & Limousine Commission (TLC). Les enregistrements viennent des fournisseurs de compteurs (TPEP) ; la TLC précise ne pas garantir leur exactitude. |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet` (par exemple `2025-01`). Page officielle : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| Accès (public, authentifié) | Public, sans authentification (téléchargement HTTP direct) |
| Format du fichier | Parquet, un fichier par mois |
| Fréquence de publication | Mensuelle |
| Délai entre la période couverte et la publication | Environ 2 mois, d'après la page de la TLC (non mesuré) |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59 158 238 octets (59,2 Mo, soit 56,4 Mio) | 3 475 226 | 20 | Taille : `os.path.getsize`. Lignes : DuckDB `SELECT COUNT(*) FROM 'yellow_tripdata_2025-01.parquet'`. Colonnes : DuckDB `DESCRIBE SELECT * FROM 'yellow_tripdata_2025-01.parquet'` |

Février et mars 2025 seront mesurés au jour 2, au moment du chargement.

## Colonnes

Types vus par DuckDB (`INTEGER` = 32 bits, `BIGINT` = 64 bits). Exemples : première ligne du fichier.

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | INTEGER | Fournisseur du compteur qui a transmis l'enregistrement (voir codes) | 1 |
| `tpep_pickup_datetime` | TIMESTAMP | Date et heure d'enclenchement du compteur (prise en charge) | 2025-01-01 00:18:38 |
| `tpep_dropoff_datetime` | TIMESTAMP | Date et heure d'arrêt du compteur (dépose) | 2025-01-01 00:26:59 |
| `passenger_count` | BIGINT | Nombre de passagers | 1 |
| `trip_distance` | DOUBLE | Distance du trajet en miles, donnée par le compteur | 1.6 |
| `RatecodeID` | BIGINT | Code tarifaire en vigueur à la fin du trajet (voir codes) | 1 |
| `store_and_fwd_flag` | VARCHAR | Trajet gardé en mémoire dans le véhicule avant envoi (pas de connexion) | N |
| `PULocationID` | INTEGER | Zone TLC de prise en charge | 229 |
| `DOLocationID` | INTEGER | Zone TLC de dépose | 237 |
| `payment_type` | BIGINT | Mode de paiement (voir codes) | 1 |
| `fare_amount` | DOUBLE | Tarif temps et distance calculé par le compteur | 10.0 |
| `extra` | DOUBLE | Suppléments divers | 3.5 |
| `mta_tax` | DOUBLE | Taxe MTA déclenchée selon le tarif appliqué | 0.5 |
| `tip_amount` | DOUBLE | Pourboire, rempli automatiquement pour les cartes ; pourboires en espèces exclus | 3.0 |
| `tolls_amount` | DOUBLE | Total des péages | 0.0 |
| `improvement_surcharge` | DOUBLE | Supplément « amélioration », prélevé à la prise en charge | 1.0 |
| `total_amount` | DOUBLE | Total facturé au passager, pourboires en espèces exclus | 18.0 |
| `congestion_surcharge` | DOUBLE | Supplément de congestion de l'État de New York | 2.5 |
| `Airport_fee` | DOUBLE | Frais d'aéroport, uniquement pour une prise en charge à LaGuardia ou JFK | 0.0 |
| `cbd_congestion_fee` | DOUBLE | Frais par trajet de la zone de congestion de la MTA, depuis le 5 janvier 2025 | 0.0 |

Source des définitions : dictionnaire TLC du 18 mars 2025 (`data_dictionary_trip_records_yellow.pdf`). Le fichier écrit `Airport_fee` avec un A majuscule, le dictionnaire `airport_fee`.

## Codes

Entre parenthèses : nombre de lignes dans le fichier de janvier.

| Colonne | Valeur | Signification |
|---|---|---|
| `VendorID` | 1 | Creative Mobile Technologies, LLC (753 671) |
| `VendorID` | 2 | Curb Mobility, LLC (2 719 860) |
| `VendorID` | 6 | Myle Technologies Inc (489) |
| `VendorID` | 7 | Helix (1 206) |
| `RatecodeID` | 1 | Tarif standard (2 756 472) |
| `RatecodeID` | 2 | JFK (94 420) |
| `RatecodeID` | 3 | Newark (8 622) |
| `RatecodeID` | 4 | Nassau ou Westchester (7 092) |
| `RatecodeID` | 5 | Tarif négocié (26 501) |
| `RatecodeID` | 6 | Course de groupe (7) |
| `RatecodeID` | 99 | Nul ou inconnu (41 963) |
| `RatecodeID` | NULL | Non renseigné dans le fichier (540 149) |
| `store_and_fwd_flag` | Y | Trajet « store and forward » (7 646) |
| `store_and_fwd_flag` | N | Pas un trajet « store and forward » (2 927 431) |
| `store_and_fwd_flag` | NULL | Non renseigné (540 149) |
| `payment_type` | 0 | Flex Fare (540 149) |
| `payment_type` | 1 | Carte bancaire (2 444 393) |
| `payment_type` | 2 | Espèces (390 429) |
| `payment_type` | 3 | Sans frais (23 773) |
| `payment_type` | 4 | Litige (76 481) |
| `payment_type` | 5 | Inconnu (1) |
| `payment_type` | 6 | Trajet annulé (aucune ligne en janvier) |

## Ce qui a surpris

- **540 149 trajets (15,5 %) ont cinq colonnes vides** : `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` et `Airport_fee`. Ce sont exactement les trajets de type `payment_type = 0` (Flex Fare). Aucun autre trajet n'a ce type de paiement.
- **Dates hors période** : les prises en charge vont du 31 décembre 2024 au 1er février 2025. 22 trajets sont hors de janvier, et 124 arrivent avant d'être partis (`dropoff` avant `pickup`).
- **Montants et distances aberrants** : `fare_amount` négatif sur 144 118 lignes, `total_amount` négatif sur 63 037 lignes et nul sur 559. Le total va de -901 $ à 863 380,37 $ pour une médiane de 19,95 $.
- **Distances et passagers** : 90 893 trajets ont une distance de 0, 162 dépassent 100 miles, 24 656 ont 0 passager.
- **`total_amount` n'est pas toujours la somme de ses composantes** : `fare + extra + mta_tax + tip + tolls + improvement_surcharge + congestion_surcharge + Airport_fee + cbd_congestion_fee` égale le total (à 1 cent près) pour 2 363 419 lignes seulement, soit 68 %. À creuser avant de conclure sur ce qui est inclus dans le total.
- **Types entiers mélangés** : `passenger_count`, `RatecodeID` et `payment_type` sont en 64 bits, `VendorID` et les zones en 32 bits. Le brief prévient qu'une même colonne peut changer de type d'un mois à l'autre : en RAW, il faudra des types larges.
