-- Outil 5 — Le seuil d'entree dans le top 150, heure par heure
--
-- Le viewer_count du dernier stream classe : combien il faut reunir pour
-- figurer dans le top a cet instant. C'est un barometre d'activite de
-- l'ecosysteme qui ne depend pas des gros streamers.
--
-- ATTENTION : MIN() est fragile — 12 observations de la base sont a 0 viewer
-- (streams qui viennent de s'arreter, artefact de l'API). Un seul 0 ecrase le
-- MIN d'un snapshot. On regarde donc aussi la mediane des seuils par heure.

WITH par_snap AS (
  SELECT captured_at,
         MIN(viewer_count) AS seuil,
         SUM(viewer_count) AS audience
  FROM observations
  GROUP BY captured_at
),
classe AS (
  SELECT CAST(strftime('%H', datetime(captured_at, '+2 hours')) AS INT) AS heure_paris,
         seuil, audience,
         ROW_NUMBER() OVER (PARTITION BY strftime('%H', datetime(captured_at, '+2 hours'))
                            ORDER BY seuil) AS rn,
         COUNT(*)     OVER (PARTITION BY strftime('%H', datetime(captured_at, '+2 hours'))) AS n
  FROM par_snap
)
SELECT heure_paris,
       MAX(n)                 AS nb_snapshots,
       ROUND(AVG(seuil))      AS seuil_median,
       ROUND(AVG(audience))   AS audience_mediane
FROM classe
WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
GROUP BY heure_paris
ORDER BY heure_paris;
