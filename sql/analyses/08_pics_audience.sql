-- Outil 8 — Detection de pics (raids, events, apparitions)
--
-- Plus fortes hausses d'audience entre deux captures consecutives d'une meme
-- diffusion. Un bond de plusieurs dizaines de milliers en 10 minutes n'est pas
-- une croissance organique : c'est un raid, un event, ou une redirection.

WITH pas AS (
  SELECT o.diffusion_id, o.captured_at, o.viewer_count,
         LAG(o.viewer_count) OVER (PARTITION BY o.diffusion_id ORDER BY o.captured_at) AS vc_prec
  FROM observations o
)
SELECT s.display_name,
       p.captured_at,
       p.vc_prec                  AS avant,
       p.viewer_count             AS apres,
       p.viewer_count - p.vc_prec AS gain
FROM pas p
JOIN diffusions d ON d.id = p.diffusion_id
JOIN streamers  s ON s.user_id = d.user_id
WHERE p.vc_prec IS NOT NULL
ORDER BY gain DESC
LIMIT 20;
