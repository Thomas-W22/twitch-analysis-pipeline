-- Outil 2 — Qui change son titre en cours de live
--
-- Les gros changeurs sont typiquement les streams "24h" et les marathons,
-- qui actualisent leur titre au fil des segments.

WITH pas AS (
  SELECT o.diffusion_id, o.title,
         LAG(o.title) OVER (PARTITION BY o.diffusion_id ORDER BY o.captured_at) AS titre_prec
  FROM observations o
)
SELECT s.display_name, COUNT(*) AS nb_changements
FROM pas p
JOIN diffusions d ON d.id = p.diffusion_id
JOIN streamers  s ON s.user_id = d.user_id
WHERE titre_prec IS NOT NULL AND p.title <> titre_prec
GROUP BY s.user_id
ORDER BY nb_changements DESC
LIMIT 20;
