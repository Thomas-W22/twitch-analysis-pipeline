-- Outil 12 — Effet du tag DropsActives sur l'audience
--
-- Les Drops sont un mecanisme promotionnel (recompenses en jeu pour les
-- spectateurs). Correle-t-il avec une audience superieure ?
--
-- BIAIS A GARDER EN TETE : les Drops sont surtout actives sur les gros jeux
-- esport. La difference observee melange l'effet du mecanisme et l'effet du
-- jeu. Pour trancher il faudrait comparer a jeu constant (ajouter game_id au
-- GROUP BY) — exercice laisse ouvert.

WITH marque AS (
  SELECT o.id, o.viewer_count,
         MAX(CASE WHEN ot.tag_name = 'DropsActivés' THEN 1 ELSE 0 END) AS drops
  FROM observations o
  LEFT JOIN observation_tags ot ON ot.observation_id = o.id
  GROUP BY o.id
),
classe AS (
  SELECT drops, viewer_count,
         ROW_NUMBER() OVER (PARTITION BY drops ORDER BY viewer_count) AS rn,
         COUNT(*)     OVER (PARTITION BY drops)                       AS n
  FROM marque
)
SELECT CASE drops WHEN 1 THEN 'Drops actives' ELSE 'sans Drops' END AS type,
       MAX(n)                    AS nb_obs,
       ROUND(AVG(viewer_count))  AS viewers_median
FROM classe
WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
GROUP BY drops;
