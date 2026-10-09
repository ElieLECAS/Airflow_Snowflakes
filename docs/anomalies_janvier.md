# Trajets anormaux de janvier 2025 : mes comptages et `MART_DATA_QUALITY`

Objectif : compter moi-même les trajets anormaux d'un mois, et comprendre pourquoi mon total diffère de celui du pipeline quand je compte chaque règle séparément.

## Méthode

Trois requêtes, dans `snowflake/verifications_finales.sql` (partie A), toutes sur janvier 2025 (3 475 226 trajets) :

| Requête | Ce qu'elle compte |
|---|---|
| A1 | chaque règle de rejet **séparément** (sur `STG_TLC__YELLOW_TRIPS`) |
| A2 | les trajets qui échouent à **au moins une** règle, chacun compté une seule fois |
| A3 | ce que contient `MART_DATA_QUALITY` pour janvier : **une seule raison par trajet**, la première règle qui échoue |

Les règles et leurs seuils sont ceux de `int_trips__flagged.sql` (100 miles, 180 minutes). L'ordre dans lequel le pipeline les teste est : `timestamp_null`, `duration_non_positive`, `duration_too_long`, `pickup_outside_file_month`, `distance_out_of_range`, `amount_non_positive`, `zone_null`.

## Résultat

| Règle | Comptée seule (A1) | Dans le pipeline (A3) | Écart |
|---|---|---|---|
| `timestamp_null` | 0 | 0 | 0 |
| `duration_non_positive` | 2 051 | 2 051 | 0 |
| `duration_too_long` | 1 377 | 1 377 | 0 |
| `pickup_outside_file_month` | 22 | 22 | 0 |
| `distance_out_of_range` | 91 055 | 90 327 | 728 |
| `amount_non_positive` | 144 998 | 130 112 | **14 886** |
| `zone_null` | 0 | 0 | 0 |
| **Somme des règles** | **239 503** | **223 889** | **15 614** |
| **Trajets écartés, comptés une fois (A2)** | **223 889** | | |

- Le total écarté du pipeline (130 112 + 90 327 + 2 051 + 1 377 + 22 = **223 889**) est **identique** à A2 : les deux comptent chaque trajet écarté une seule fois. Cela représente 6,44 % du fichier ; les 3 251 337 trajets restants sont valides.
- La somme des règles comptées séparément (239 503) est **plus grande** de 15 614, parce qu'un trajet peut échouer à plusieurs règles.

## D'où vient l'écart

Le pipeline donne à chaque trajet **une seule raison : la première règle qui échoue**. Un trajet qui échoue à deux règles n'est donc compté que pour la plus haute dans la liste. Les écarts se concentrent sur les deux dernières règles de la liste, et je les ai vérifiés :

| Écart | Cause | Trajets |
|---|---|---|
| `distance_out_of_range` : 728 | trajets qui échouent aussi à une règle de durée ou de mois, placée avant | 728 |
| `amount_non_positive` : 14 886 | trajets qui échouent aussi à une règle de durée ou de mois | 70 |
| | trajets qui échouent aussi à la règle de **distance**, placée avant (distance nulle ou supérieure à 100 miles **et** montant négatif ou nul) | 14 816 |

728 + 14 886 = 15 614, soit exactement l'écart. Les règles de durée, de mois et d'horodatage ont les mêmes comptes dans A1 et A3 : elles passent avant les autres, donc aucun de leurs trajets ne leur est « retiré ».

Le gros chevauchement (14 816 trajets) est celui des trajets à distance nulle et montant négatif : des courses annulées ou remboursées, qui échouent à ces deux règles à la fois.

## Ce qu'il faut retenir

- Pour répondre à « combien de trajets sont écartés ? », il faut compter des **trajets**, pas additionner des règles : 223 889 sur 3 475 226 en janvier.
- Pour répondre à « pourquoi sont-ils écartés ? », `MART_DATA_QUALITY` donne **la première cause** de chaque trajet. La raison `amount_non_positive` y est sous-estimée par rapport à ce qu'on obtient en la comptant seule, et c'est normal.
- Changer l'ordre des règles dans `int_trips__flagged.sql` changerait la répartition par raison, mais pas le total écarté.
