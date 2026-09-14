-- =============================================================================
-- Niveau 1 : requêtes de base (SELECT, WHERE, ORDER BY, LIMIT)
--
-- Pour exécuter ces requêtes :
--   sqlite3 data/processed/twitch.db
--   puis coller la requête, ou : .read sql/queries/01_bases.sql
-- Confort d'affichage (à taper une fois dans le shell sqlite3) :
--   .mode column
--   .headers on
-- =============================================================================


-- -----------------------------------------------------------------------------
-- EXEMPLE RÉSOLU — Top 10 des streams par nombre de viewers
--
-- L'ordre d'écriture d'une requête SQL est toujours le même :
--   SELECT   quelles colonnes je veux voir
--   FROM     dans quelle table
--   WHERE    quelles lignes je garde        (optionnel)
--   ORDER BY dans quel ordre je les affiche (optionnel)
--   LIMIT    combien j'en affiche           (optionnel)
--
-- Mais l'ordre d'EXÉCUTION par le moteur est différent : FROM -> WHERE ->
-- SELECT -> ORDER BY -> LIMIT. C'est pour ça qu'on filtre sur des colonnes
-- de la table (WHERE), et qu'on trie sur ce qu'on a sélectionné (ORDER BY).
-- -----------------------------------------------------------------------------
SELECT title, viewer_count, language
FROM observations
ORDER BY viewer_count DESC   -- DESC = décroissant, ASC = croissant (défaut)
LIMIT 10;


-- -----------------------------------------------------------------------------
-- EXERCICE 1 — Les streams en français uniquement
-- Affiche title et viewer_count des observations dont language = 'fr',
-- triés du plus gros au plus petit, limités à 20.
-- Rappel : les chaînes se comparent avec des guillemets simples ('fr').
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 2 — Les gros streams
-- Affiche toutes les colonnes (SELECT *) des observations avec plus de
-- 20000 viewers.
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 3 — Combiner deux conditions
-- Streams en français ET avec plus de 5000 viewers.
-- Mot-clé à utiliser : AND (il existe aussi OR, et NOT).
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 4 — Les streams sans jeu renseigné
-- Rappel du schéma : game_id est NULL quand le stream n'a pas de jeu associé.
-- PIÈGE CLASSIQUE : on n'écrit PAS `WHERE game_id = NULL` (ça ne renvoie
-- jamais rien — NULL n'est égal à rien, pas même à lui-même).
-- La bonne syntaxe est `WHERE game_id IS NULL` (ou IS NOT NULL).
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 5 — Recherche textuelle
-- Trouve les streams dont le titre contient le mot "LCK".
-- Mot-clé : LIKE avec le joker % — `WHERE title LIKE '%LCK%'` signifie
-- "n'importe quoi, puis LCK, puis n'importe quoi".
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 6 — Liste des langues présentes, sans doublon
-- Affiche chaque langue une seule fois.
-- Mot-clé : SELECT DISTINCT (même idée que la dédup en Python, mais côté SQL).
-- -----------------------------------------------------------------------------
-- TODO


-- -----------------------------------------------------------------------------
-- EXERCICE 7 — Explorer les autres tables
-- Les 6 tables existent : games, streamers, diffusions, observations, tags,
-- observation_tags. Écris un SELECT simple sur streamers et sur games pour
-- voir à quoi ressemblent leurs lignes (utile avant d'attaquer les jointures
-- au niveau 03).
-- -----------------------------------------------------------------------------
-- TODO
