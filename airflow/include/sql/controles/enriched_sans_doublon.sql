-- Le mois traité contient des trajets valides, et aucun trajet n'apparaît deux fois (une clé trip_sk par ligne).
-- La TLC ne fournit pas d'identifiant de trajet : trip_sk est un MD5 de la clé métier, et int_trips__enriched
-- garde la première version d'un trajet présent deux fois. Un doublon ici fausserait les comptages des marts.
SELECT
    COUNT(*) > 0,                                -- le mois contient des trajets valides
    COUNT(*) = COUNT(DISTINCT trip_sk)           -- aucun trajet en double
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__ENRICHED
WHERE source_file_month = '{{ ds }}'::date;
