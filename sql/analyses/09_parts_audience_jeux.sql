-- Outil 9 — Parts d'audience des jeux, jour par jour
--
-- Detecte les hypes : un jeu qui passe de 0,1 % a 8 % en 24h signale une
-- sortie ou un event (la semaine de collecte couvrait la Gamescom).

WITH tot_jour AS (
  SELECT date(captured_at) AS jour, SUM(viewer_count) AS total
  FROM observations
  GROUP BY jour
),
top_jeux AS (
  SELECT game_id
  FROM observations
  WHERE game_id IS NOT NULL
  GROUP BY game_id
  ORDER BY SUM(viewer_count) DESC
  LIMIT 5
)
SELECT date(o.captured_at) AS jour,
       g.name,
       ROUND(100.0 * SUM(o.viewer_count) / t.total, 1) AS pct
FROM observations o
JOIN games    g ON g.game_id = o.game_id
JOIN tot_jour t ON t.jour = date(o.captured_at)
WHERE o.game_id IN (SELECT game_id FROM top_jeux)
GROUP BY jour, g.game_id
ORDER BY jour, pct DESC;
