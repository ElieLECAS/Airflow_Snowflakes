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
