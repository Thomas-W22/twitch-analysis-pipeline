-- Outil 10 — Prime time par jeu
--
-- Repartition horaire de l'audience de chaque jeu, en % de son propre total.
-- Normaliser par jeu rend comparables un jeu a 15 % de part et un a 3 % :
-- on compare des FORMES horaires, pas des volumes.

WITH top_jeux AS (
  SELECT game_id
  FROM observations
  WHERE game_id IS NOT NULL
  GROUP BY game_id
  ORDER BY SUM(viewer_count) DESC
  LIMIT 6
),
par_heure AS (
  SELECT o.game_id,
         CAST(strftime('%H', datetime(o.captured_at, '+2 hours')) AS INT) AS heure_paris,
         SUM(o.viewer_count) AS audience
  FROM observations o
  WHERE o.game_id IN (SELECT game_id FROM top_jeux)
  GROUP BY o.game_id, heure_paris
)
SELECT g.name,
       p.heure_paris,
       ROUND(100.0 * p.audience
             / SUM(p.audience) OVER (PARTITION BY p.game_id), 2) AS pct_du_jeu
FROM par_heure p
JOIN games g ON g.game_id = p.game_id
ORDER BY g.name, p.heure_paris;
