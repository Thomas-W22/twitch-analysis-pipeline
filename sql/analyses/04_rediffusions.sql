-- Outil 4 — Rediffusions vs lives
--
-- Le tag 'Rediffusion' permet d'isoler les replays, qui polluent toute
-- analyse d'audience "live". A exclure en amont des autres analyses si on
-- veut ne mesurer que du direct.

WITH marque AS (
  SELECT o.id, o.viewer_count,
         MAX(CASE WHEN ot.tag_name = 'Rediffusion' THEN 1 ELSE 0 END) AS est_redif
  FROM observations o
  LEFT JOIN observation_tags ot ON ot.observation_id = o.id
  GROUP BY o.id
)
SELECT CASE est_redif WHEN 1 THEN 'rediffusion' ELSE 'live' END AS type,
       COUNT(*)                  AS nb_obs,
       ROUND(AVG(viewer_count), 1) AS viewers_moyen,
       MAX(viewer_count)         AS viewers_max
FROM marque
GROUP BY est_redif;
