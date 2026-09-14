-- Outil 3 — Courbe de vie type d'une diffusion
--
-- Pour chaque observation on calcule l'anciennete du live (captured_at moins
-- started_at), puis on exprime l'audience en % du pic de CETTE diffusion.
-- Normaliser par le pic rend comparables un stream a 200 viewers et un a 50 000.
--
-- LIMITE : sessions censurees des deux cotes — certaines avaient commence
-- avant le debut de la collecte, d'autres continuaient apres sa fin.

WITH bornes AS (
  SELECT o.diffusion_id, o.viewer_count,
         CAST((julianday(o.captured_at) - julianday(d.started_at)) * 24 AS INT) AS h_live,
         MAX(o.viewer_count) OVER (PARTITION BY o.diffusion_id) AS pic
  FROM observations o
  JOIN diffusions d ON d.id = o.diffusion_id
)
SELECT h_live,
       COUNT(*)                            AS nb_obs,
       ROUND(AVG(100.0 * viewer_count / pic), 1) AS pct_du_pic
FROM bornes
WHERE pic > 0 AND h_live BETWEEN 0 AND 15
GROUP BY h_live
ORDER BY h_live;
