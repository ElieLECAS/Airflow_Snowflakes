-- Le mois traité contient des trajets, et la part de trajets écartés par int_trips__flagged reste sous le seuil.
-- Seuil : paramètre max_pct_rejets du DAG (10 %). Janvier, février et mars 2025 rejettent 6,4 %, 7,6 % et 7,7 % :
-- le seuil laisse une marge, mais un doublement des anomalies arrêterait le pipeline avant int_trips__enriched.
SELECT
    COUNT(*) > 0,                                                                  -- le mois a bien été marqué
    COUNT_IF(rejection_reason IS NOT NULL) / COUNT(*) * 100 < {{ params.max_pct_rejets }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
