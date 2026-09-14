-- =============================================================================
-- MPD — Twitch SQL project
-- Traduction du MLD validé (6 entités) en DDL SQLite.
--
--   STREAMERS (1,N) ── RÉALISE ──> (1,1) DIFFUSIONS
--   DIFFUSIONS (1,N) ── COMPORTE ──> (1,1) OBSERVATIONS
--   GAMES (0,N) ── CONCERNE ──> (0,1) OBSERVATIONS
--   OBSERVATIONS (0,N) ── OBSERVATION_TAGS ──> (0,N) TAGS
--
-- Rappel : SQLite n'applique les FOREIGN KEY que si la connexion active
-- `PRAGMA foreign_keys = ON;` (à faire dans load.py à chaque connexion, ça ne
-- se stocke pas dans le fichier .db).
--
-- Décisions prises sur les 3 questions ouvertes de la version précédente :
--   1. Dates (started_at, captured_at) : TEXT ISO-8601 UTC, format natif de
--      l'API ("2026-08-19T18:58:14Z"), compatible avec date()/strftime()/
--      julianday() de SQLite pour la heatmap heure × jour. Pas de conversion
--      au chargement.
--   2. Tables STRICT (SQLite >= 3.37, tu es en 3.45) : activées pour un typage
--      réel plutôt que l'affinité laxiste par défaut — utile pour apprendre
--      avec des erreurs qui remontent tôt.
--   3. is_mature / thumbnail_url / type : confirmés hors scope, non repris.
-- =============================================================================

PRAGMA foreign_keys = ON;


-- -----------------------------------------------------------------------------
-- GAMES — dimension.
-- Clé naturelle : game_id fourni par l'API (TEXT, chaîne de chiffres côté
-- Twitch — on ne la convertit pas en INTEGER).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS games (
    game_id     TEXT NOT NULL PRIMARY KEY,
    name        TEXT NOT NULL,
    box_art_url TEXT
) STRICT;


-- -----------------------------------------------------------------------------
-- STREAMERS — dimension.
-- login/display_name : toujours présents dans /helix/streams (jamais vides
-- sur les champs observés), donc NOT NULL.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS streamers (
    user_id      TEXT NOT NULL PRIMARY KEY,
    login        TEXT NOT NULL,
    display_name TEXT NOT NULL
) STRICT;


-- -----------------------------------------------------------------------------
-- DIFFUSIONS — une session de live. Ne contient QUE ce qui ne varie pas
-- pendant la session.
-- id = stream id fourni directement par l'API (pas de clé technique générée).
-- user_id NOT NULL : la patte (1,1) de RÉALISE côté DIFFUSIONS l'impose.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diffusions (
    id         TEXT NOT NULL PRIMARY KEY,
    started_at TEXT NOT NULL,
    user_id    TEXT NOT NULL REFERENCES streamers(user_id)
) STRICT;


-- -----------------------------------------------------------------------------
-- OBSERVATIONS — table de faits : une photo d'une diffusion à un instant donné.
-- Tout ce qui peut varier au cours d'une diffusion vit ici.
--
-- id : seule clé technique du schéma (INTEGER PRIMARY KEY = alias du rowid,
-- auto-incrémenté par SQLite sur INSERT avec id = NULL).
-- game_id nullable : patte (0,1) de CONCERNE. transform.py doit convertir un
-- game_id vide ("") du JSON brut en NULL avant le chargement.
-- UNIQUE (diffusion_id, captured_at) : rend le chargement idempotent — un
-- double-run de load.py sur le même fichier brut ne duplique pas la ligne
-- (à utiliser avec INSERT OR IGNORE côté load.py).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observations (
    id            INTEGER PRIMARY KEY,
    viewer_count  INTEGER NOT NULL,
    title         TEXT,
    language      TEXT,
    captured_at   TEXT NOT NULL,
    diffusion_id  TEXT NOT NULL REFERENCES diffusions(id),
    game_id       TEXT REFERENCES games(game_id),
    UNIQUE (diffusion_id, captured_at)
) STRICT;


-- -----------------------------------------------------------------------------
-- TAGS — dimension à clé naturelle (tag_name), car tag_ids est vide dans les
-- données réelles.
-- COLLATE NOCASE sur la PK : évite que "Français" et "français" soient deux
-- lignes distinctes (comparaison TEXT sensible à la casse par défaut en SQLite).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tags (
    tag_name TEXT NOT NULL PRIMARY KEY COLLATE NOCASE
) STRICT;


-- -----------------------------------------------------------------------------
-- OBSERVATION_TAGS — table de liaison de l'association (0,N)—(0,N).
-- PK composite (observation_id, tag_name) : garantit qu'un même tag n'est pas
-- lié deux fois à la même observation, et sert d'index de jointure naturel.
-- ON DELETE CASCADE sur observation_id : si une observation est supprimée
-- (purge, correction de données), ses liaisons de tags le sont aussi — évite
-- des lignes orphelines. Pas de CASCADE sur tag_name : un tag reste une
-- dimension, on ne le supprime pas en pratique.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observation_tags (
    observation_id INTEGER NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
    tag_name        TEXT    NOT NULL REFERENCES tags(tag_name),
    PRIMARY KEY (observation_id, tag_name)
) STRICT;


-- -----------------------------------------------------------------------------
-- INDEX — les colonnes de FK ne sont PAS indexées automatiquement par SQLite
-- (contrairement à MySQL) : sans ces index, chaque jointure et chaque
-- ON DELETE CASCADE fait un scan complet de la table référencée.
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_observations_captured_at  ON observations(captured_at);
CREATE INDEX IF NOT EXISTS idx_observations_diffusion_id ON observations(diffusion_id);
CREATE INDEX IF NOT EXISTS idx_observations_game_id      ON observations(game_id);
CREATE INDEX IF NOT EXISTS idx_diffusions_user_id        ON diffusions(user_id);
