-- Outil 7 — Regularite des streamers : le seuil pour la phase 2 (Option B)
--
-- Combien de streamers faut-il garder pour couvrir l'essentiel de l'audience ?
-- Repond directement au "GROUP BY user_id HAVING COUNT(*) > seuil" prevu au
-- programme de la phase 2.

WITH par_streamer AS (
  SELECT d.user_id,
         COUNT(*)             AS nb_obs,
         SUM(o.viewer_count)  AS audience
  FROM observations o
  JOIN diffusions d ON d.id = o.diffusion_id
  GROUP BY d.user_id
)
SELECT CASE
         WHEN nb_obs >= 500 THEN 'a. >= 500 obs'
         WHEN nb_obs >= 200 THEN 'b. 200-499'
         WHEN nb_obs >= 100 THEN 'c. 100-199'
         WHEN nb_obs >=  50 THEN 'd. 50-99'
         WHEN nb_obs >=  20 THEN 'e. 20-49'
         ELSE                    'f. < 20 (ponctuel)'
       END                  AS tranche,
       COUNT(*)             AS nb_streamers,
       SUM(nb_obs)          AS total_obs,
       ROUND(100.0 * SUM(audience)
             / (SELECT SUM(viewer_count) FROM observations), 1) AS pct_audience
FROM par_streamer
GROUP BY tranche
ORDER BY tranche;
