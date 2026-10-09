# Trajets rejetés en janvier 2025

On a compté à la main les trajets anormaux de janvier (3 475 226 trajets) pour comparer avec `MART_DATA_QUALITY`, et comprendre pourquoi la somme des règles comptées une par une dépasse le total rejeté par le pipeline.

## Les trois requêtes

Elles sont dans `snowflake/verifications_finales.sql`, partie A.

- A1 compte chaque règle séparément, sur `STG_TLC__YELLOW_TRIPS`.
- A2 compte les trajets qui échouent à au moins une règle, chacun une seule fois.
- A3 lit `MART_DATA_QUALITY` : une seule raison par trajet, la première règle qui échoue.

Les règles et les seuils (100 miles, 180 minutes) sont ceux de `int_trips__flagged.sql`. Le pipeline les teste dans cet ordre : `timestamp_null`, `duration_non_positive`, `duration_too_long`, `pickup_outside_file_month`, `distance_out_of_range`, `amount_non_positive`, `zone_null`.

## Résultat

| Règle | Seule (A1) | Dans le pipeline (A3) | Écart |
|---|---|---|---|
| `timestamp_null` | 0 | 0 | 0 |
| `duration_non_positive` | 2 051 | 2 051 | 0 |
| `duration_too_long` | 1 377 | 1 377 | 0 |
| `pickup_outside_file_month` | 22 | 22 | 0 |
| `distance_out_of_range` | 91 055 | 90 327 | 728 |
| `amount_non_positive` | 144 998 | 130 112 | 14 886 |
| `zone_null` | 0 | 0 | 0 |
| Somme | 239 503 | 223 889 | 15 614 |

A2 donne 223 889 trajets rejetés, comme le total de A3 (130 112 + 90 327 + 2 051 + 1 377 + 22). C'est 6,44 % du fichier, il reste 3 251 337 trajets valides.

## D'où vient l'écart de 15 614

Un trajet qui échoue à plusieurs règles n'est compté que pour la première de la liste. Seules les deux dernières règles de la liste perdent des trajets :

- `distance_out_of_range` perd 728 trajets, qui échouent aussi à une règle de durée ou de mois passée avant.
- `amount_non_positive` perd 14 886 trajets : 70 échouent aussi à une règle de durée ou de mois, et 14 816 à la règle de distance (distance nulle ou supérieure à 100 miles, avec un montant négatif ou nul). Ce sont surtout des courses annulées ou remboursées.

728 + 14 886 = 15 614, c'est bien l'écart. Les règles qui passent en premier ont les mêmes comptes dans A1 et A3, personne ne leur retire de trajet.

## À retenir

Pour savoir combien de trajets sont écartés, on compte des trajets, pas des règles : 223 889 en janvier. Pour savoir pourquoi, `MART_DATA_QUALITY` donne la première cause de chaque trajet, ce qui sous-estime `amount_non_positive` par rapport à un comptage seul. Changer l'ordre des règles changerait la répartition par raison, pas le total.
