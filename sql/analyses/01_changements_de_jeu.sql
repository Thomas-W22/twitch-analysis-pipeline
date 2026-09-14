-- Outil 1 — Effet d'un changement de jeu en cours de diffusion
--
-- Compare deux captures consecutives d'une meme diffusion : le streamer
-- a-t-il change de jeu, et qu'est devenue son audience ?
--
-- PIEGE STATISTIQUE : la moyenne des pourcentages est trompeuse ici. Un stream
-- qui passe de 10 a 20 viewers pese +100 %, autant qu'un gros stream qui gagne
-- 50 %. On filtre donc les petits streams (vc_prec >= 30) ET on prend la
-- MEDIANE, pas la moyenne. Avec la moyenne on lisait "+7,8 % quand on change
-- de jeu", conclusion inverse de la realite.
--
-- Lecture : la ligne "meme jeu" est le temoin de comparaison.

WITH pas AS (
  SELECT o.diffusion_id, o.game_id, o.viewer_count,
         LAG(o.game_id)      OVER (PARTITION BY o.diffusion_id ORDER BY o.captured_at) AS game_prec,
         LAG(o.viewer_count) OVER (PARTITION BY o.diffusion_id ORDER BY o.captured_at) AS vc_prec
  FROM observations o
),
etapes AS (
  SELECT CASE WHEN game_id <> game_prec THEN 'changement de jeu' ELSE 'meme jeu' END AS situation,
         viewer_count - vc_prec                     AS delta,
         100.0 * (viewer_count - vc_prec) / vc_prec AS pct
  FROM pas
  WHERE game_prec IS NOT NULL AND game_id IS NOT NULL AND vc_prec >= 30
),
classe AS (
  SELECT situation, delta, pct,
         ROW_NUMBER() OVER (PARTITION BY situation ORDER BY pct) AS rn,
         COUNT(*)     OVER (PARTITION BY situation)              AS n
  FROM etapes
)
SELECT situation,
       MAX(n)               AS nb_pas,
       ROUND(AVG(pct), 2)   AS pct_median,
       ROUND(AVG(delta), 1) AS delta_median
FROM classe
WHERE rn IN ((n + 1) / 2, (n + 2) / 2)
GROUP BY situation;
