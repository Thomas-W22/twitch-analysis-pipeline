-- Outil 6 — Rotation : qui entre dans le top 150 chaque jour
--
-- Un streamer "nouveau" est vu pour la premiere fois ce jour-la. Le taux
-- decroit mecaniquement au fil de la collecte (le stock de deja-vus grandit) :
-- c'est attendu, pas un bug. Ce qui compte est le plancher atteint.

WITH presence AS (
  SELECT DISTINCT d.user_id, date(o.captured_at) AS jour
  FROM observations o
  JOIN diffusions d ON d.id = o.diffusion_id
),
premiere AS (
  SELECT user_id, MIN(jour) AS premier_jour FROM presence GROUP BY user_id
)
SELECT p.jour,
       COUNT(*)                                                    AS streamers_presents,
       SUM(CASE WHEN pr.premier_jour = p.jour THEN 1 ELSE 0 END)   AS nouveaux,
       ROUND(100.0 * SUM(CASE WHEN pr.premier_jour = p.jour THEN 1 ELSE 0 END)
             / COUNT(*), 1)                                        AS pct_nouveaux
FROM presence p
JOIN premiere pr ON pr.user_id = p.user_id
GROUP BY p.jour
ORDER BY p.jour;
