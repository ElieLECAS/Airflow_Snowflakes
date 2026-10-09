# Réponse à la direction d'Hudson Cab Partners

Données : trajets de taxis jaunes de janvier à mars 2025 (source TLC), chargés et contrôlés par le pipeline du dépôt.

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

Elle classe les combinaisons zone, heure et type de jour (semaine ou week-end) par nombre moyen de trajets par jour, garde les dix premières, puis calcule ce que rapporte un trajet payé par carte et un trajet payé en espèces. Elle lit `MART_ZONE_HOURLY_DEMAND`, `FCT_TRIPS` et `DIM_PAYMENT_TYPE`. On la retrouve dans `snowflake/requete_direction.sql`.

```sql
-- Où et quand la demande de taxis jaunes est-elle la plus forte, et combien rapporte un trajet
-- selon le mode de paiement ? Période : janvier à mars 2025.
--
-- Classement : trajets PAR JOUR, pas le total. Le trimestre compte 64 jours de semaine et 26 jours de
-- week-end : classer sur le total écraserait mécaniquement le week-end.
-- Les zones 264 (inconnue) et 265 (hors de New York) sont écartées du classement.

WITH top_cellules AS (                       -- les 10 couples zone x heure x type de jour les plus demandés
    SELECT
        pickup_zone_key,
        pickup_zone_name,
        pickup_borough,
        pickup_hour,
        is_weekend,
        nb_trips,
        avg_trips_per_day,
        avg_revenue_per_trip,
        revenue_per_hour_driven
    FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
    WHERE pickup_zone_key NOT IN (264, 265)
    QUALIFY ROW_NUMBER() OVER (ORDER BY avg_trips_per_day DESC) <= 10
),

par_paiement AS (                            -- pour ces 10 cellules, ce que rapporte un trajet selon le paiement
    SELECT
        f.pickup_zone_key,
        f.pickup_hour,
        f.is_weekend,
        ROUND(AVG(IFF(p.payment_type_label = 'Credit card', f.total_amount, NULL)), 2) AS avg_total_carte,
        ROUND(AVG(IFF(p.payment_type_label = 'Cash',        f.total_amount, NULL)), 2) AS avg_total_especes
    FROM NYC_TAXI.MARTS.FCT_TRIPS f
    JOIN top_cellules t
      ON  t.pickup_zone_key = f.pickup_zone_key
      AND t.pickup_hour     = f.pickup_hour
      AND t.is_weekend      = f.is_weekend
    LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p ON p.payment_type_key = f.payment_type_key
    GROUP BY 1, 2, 3
)

SELECT
    t.pickup_zone_name                         AS zone,
    t.pickup_borough                           AS arrondissement,
    t.pickup_hour                              AS heure,
    IFF(t.is_weekend, 'week-end', 'semaine')   AS type_de_jour,
    t.avg_trips_per_day                        AS trajets_par_jour,
    t.nb_trips                                 AS trajets_sur_le_trimestre,
    t.avg_revenue_per_trip                     AS revenu_moyen_par_trajet,
    p.avg_total_carte                          AS revenu_moyen_carte,
    p.avg_total_especes                        AS revenu_moyen_especes,
    t.revenue_per_hour_driven                  AS revenu_par_heure_conduite
FROM top_cellules t
JOIN par_paiement p
  ON  p.pickup_zone_key = t.pickup_zone_key
  AND p.pickup_hour     = t.pickup_hour
  AND p.is_weekend      = t.is_weekend
ORDER BY t.avg_trips_per_day DESC;
```

## Résultat

| Zone | Heure | Type de jour | Trajets par jour | Revenu moyen par trajet | Payé par carte | Payé en espèces | Revenu par heure de course |
|---|---|---|---|---|---|---|---|
| East Village | 0 h | week-end | 738,0 | 22,84 $ | 22,75 $ | 19,99 $ | 113,75 $ |
| East Village | 1 h | week-end | 725,7 | 22,11 $ | 22,44 $ | 20,95 $ | 109,36 $ |
| Midtown Center | 18 h | semaine | 596,4 | 24,98 $ | 25,54 $ | 20,65 $ | 106,77 $ |
| Midtown Center | 17 h | semaine | 571,9 | 30,06 $ | 27,06 $ | 21,64 $ | 115,24 $ |
| West Village | 0 h | week-end | 559,1 | 23,14 $ | 23,33 $ | 20,57 $ | 108,21 $ |
| Midtown Center | 20 h | semaine | 534,5 | 22,81 $ | 23,80 $ | 18,62 $ | 107,93 $ |
| West Village | 1 h | week-end | 519,4 | 22,57 $ | 23,26 $ | 20,02 $ | 105,77 $ |
| East Village | 2 h | week-end | 519,0 | 22,00 $ | 22,73 $ | 20,33 $ | 117,25 $ |
| Midtown Center | 19 h | semaine | 512,1 | 24,14 $ | 24,70 $ | 20,05 $ | 113,12 $ |
| Midtown Center | 21 h | semaine | 494,3 | 23,01 $ | 23,97 $ | 19,04 $ | 108,84 $ |

Toutes ces zones sont à Manhattan. L'heure est celle de la prise en charge (0 h veut dire entre minuit et 1 h). Le revenu par heure de course est le total encaissé divisé par le temps passé en course, les attentes entre deux clients ne sont pas comptées.

## Ce qu'on en tire

1. La demande est concentrée à Manhattan (88 % des trajets y démarrent) et se joue à deux moments : les nuits de week-end entre minuit et 2 h à East Village et West Village (jusqu'à 738 trajets par jour à East Village), et les soirées de semaine entre 17 h et 21 h à Midtown Center (de 494 à 596 trajets par jour).
2. Un trajet rapporte 27 $ en moyenne sur le trimestre, entre 22 $ et 30 $ dans les dix créneaux les plus demandés. Un chauffeur qui enchaîne les courses dans ces créneaux encaisse environ 106 à 117 $ par heure de course.
3. Un trajet payé par carte rapporte plus qu'un trajet payé en espèces (28,32 $ contre 23,70 $), mais l'écart vient des pourboires : ils ne sont enregistrés que pour les cartes (4,14 $ en moyenne, 0 $ en espèces), alors que le tarif de la course est presque le même (18,13 $ contre 18,06 $).

## Limites

- Trois mois, en hiver : ni l'été, ni les jours fériés, ni les grands événements. Les créneaux de nuit de week-end peuvent être plus ou moins forts à une autre saison.
- 7,3 % des trajets sont écartés par les règles de qualité (815 648 sur 11 198 026) : montants nuls ou négatifs (4,4 %), distances nulles ou supérieures à 100 miles (2,6 %), durées anormales, trajets hors du mois du fichier. La réponse porte sur les 10 382 378 trajets valides.
- 23 318 trajets partent d'une zone inconnue ou hors de New York. Ils sont exclus du classement.
- Les pourboires en espèces ne sont pas enregistrés : la comparaison carte contre espèces ne compare pas les mêmes montants. De plus, 17 % des trajets sont payés en « Flex Fare » et n'apparaissent dans aucune des deux colonnes.
- « Week-end » veut dire samedi et dimanche : le créneau de 0 h du week-end correspond aux nuits de vendredi à samedi et de samedi à dimanche.
- Le classement est fait par trajets par jour, parce que le trimestre compte 64 jours de semaine et 26 jours de week-end. Avec le nombre total de trajets, le haut du classement ne contiendrait que des créneaux de semaine (Midtown Center le soir, Times Square à 21 h, l'Upper East Side en journée).
- Seuls les taxis jaunes sont mesurés, pas les taxis verts ni les voitures avec chauffeur.
