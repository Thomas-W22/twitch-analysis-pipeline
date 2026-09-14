-- Outil 11 — Variantes de tags a fusionner  (QUALITE DE DONNEES)
--
-- REQUIERT une fonction Python enregistree sur la connexion :
--     conn.create_function("norm_tag", 1, norm)
-- ou norm() met en minuscules ET retire les accents (unicodedata NFD).
-- SQLite ne sait pas retirer les accents nativement, et le COLLATE NOCASE
-- pose sur tags.tag_name ne replie que la casse ASCII.
--
-- Ce que ca revele sur les donnees reelles : quatre variantes de "Francais"
-- coexistent — la forme normale (128 088 usages), "francais" sans accent
-- (4 622), "Francais" avec un a-ogonek polonais a la place du c-cedille (229),
-- et une quatrieme VISUELLEMENT IDENTIQUE a la premiere mais encodee en NFD
-- (c + cedille combinante, 1 usage). Deux chaines d'octets differents qui
-- s'affichent pareil : impossible a reperer a l'oeil.

SELECT norm_tag(tag_name)              AS forme_normalisee,
       COUNT(*)                        AS nb_variantes,
       GROUP_CONCAT(tag_name, ' / ')   AS variantes
FROM tags
GROUP BY forme_normalisee
HAVING COUNT(*) > 1
ORDER BY nb_variantes DESC, forme_normalisee;
