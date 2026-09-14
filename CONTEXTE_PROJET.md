# Contexte — Projet Twitch SQL/ETL

Projet perso pour pratiquer la gestion de données et le SQL (pas un projet pro). Objectif :
pipeline ETL propre à partir de l'API Twitch (Helix, en direct) → SQLite → montée en
compétence SQL du niveau bases jusqu'aux window functions, puis une phase 2 ML.

## Préférence de travail importante

Je préfère écrire le code moi-même après avoir vu un exemple minimal ou un squelette.
Pour les parties qui sont un exercice d'apprentissage (pas de la boilerplate), donne-moi
des signatures de fonctions + docstrings + `TODO`/`NotImplementedError` plutôt que
l'implémentation complète, sauf si je demande explicitement le code fini. Pose-moi des
questions de conception plutôt que de trancher à ma place quand il y a un vrai choix
(ex : granularité d'une table, cardinalité d'une association).

## Où en est le projet

**Statut au 26/08/2026** : MCD, MLD et MPD (`db/schema.sql`) finalisés. Pipeline ETL
fonctionnel de bout en bout, **base entièrement chargée** : 882 fichiers, 0 erreur,
130 280 observations couvrant 6 j 16 h (19 → 26 août 2026). Douze outils d'analyse
sont écrits dans `sql/analyses/` et exécutables via `src/analyses.py`. Deux artefacts
publiés (heatmap + tableau de bord).

**Reste à faire** : les exercices SQL de `sql/queries/` — `01_bases.sql` est écrit
(exemple résolu + 7 exercices), les niveaux 02 à 05 sont vides. Volontairement remis
à plus tard.

**Chargement complet** : `src/load.py` a désormais `load_all_snapshots()` — une seule
connexion pour tous les fichiers, commit tous les 25, et un `try/except` par fichier
pour qu'un JSON corrompu n'interrompe pas les autres. Relançable sans risque.

## Scope retenu

- Source : API Twitch Helix, en direct (Client Credentials flow, pas d'auth utilisateur)
- Données : top ~150 streams par viewer_count (déjà triés par l'API)
- Fréquence de capture : toutes les 10 minutes
- Durée de collecte : ~1 semaine, pour avoir de vrais patterns temporels (jour/nuit,
  semaine/weekend)
- Pas d'orchestrateur (Airflow etc.) — volontairement écarté, disproportionné pour ce
  volume ; un simple script de polling suffit
- Le script de collecte tourne actuellement sur ma machine (PC avec mise en veille
  désactivée), staging en JSON brut horodaté dans `data/raw/`

## Structure du projet

```
twitch-sql-project/
├── src/
│   ├── auth.py          # complet : Client Credentials flow (get_access_token, get_auth_headers)
│   ├── extract.py       # fetch_top_streams (pagination) + run_polling_loop
│   ├── transform.py     # CODÉ — parsing JSON + dédup, 6 fonctions (voir plus bas)
│   └── load.py          # CODÉ — chargement idempotent des 6 tables
├── db/schema.sql        # CODÉ — MPD, 6 tables + index, tables STRICT
├── sql/queries/         # 01_bases.sql écrit (exemple résolu + 7 exercices),
│                        # 02 → 05 encore vides
├── sql/analyses/        # 12 outils d'analyse, un .sql commenté par outil
├── src/analyses.py      # exécute les .sql de sql/analyses/ (python -m src.analyses)
├── heatmap.html         # artefact publié — heatmap heure × jour
├── dashboard.html       # artefact publié — les 12 analyses
├── data/raw/            # JSON bruts horodatés (882 fichiers), alimentés par extract.py
├── data/processed/      # twitch.db (généré, gitignored)
├── .env / .env.example, .gitignore, requirements.txt (requests, python-dotenv)
```

## Modèle de données — MCD/MLD finalisés

6 entités. Décisions de conception actées (à ne pas remettre en cause sans discussion) :

- **STREAMERS** (user_id PK, login, display_name) — `broadcaster_type` a été retiré :
  ce champ n'existe pas dans la réponse de `/helix/streams` (uniquement sur
  `/helix/users`, endpoint non utilisé ici)
- **DIFFUSIONS** (id PK, started_at, user_id FK) — `id` est le stream id fourni
  directement par l'API Twitch (pas un id généré par nous). Table volontairement
  épurée : ne contient que ce qui NE change PAS pendant une session de live.
- **OBSERVATIONS** (id PK autoincrement, viewer_count, title, language, captured_at,
  diffusion_id FK, game_id FK nullable) — contient tout ce qui peut varier au cours
  d'une diffusion (un streamer change de jeu et de titre ensemble en général, donc les
  deux vivent ici, pas dans DIFFUSIONS)
- **GAMES** (game_id PK, name, box_art_url)
- **TAGS** (tag_name PK) — clé naturelle : `tag_ids` est vide dans les données réelles,
  seul `tags` (libellés) est peuplé
- **OBSERVATION_TAGS** (observation_id PK+FK, tag_name PK+FK) — table de liaison,
  association (0,N)—(0,N) entre OBSERVATIONS et TAGS

Cardinalités des associations simples :
- STREAMERS (1,N) — RÉALISE — (1,1) DIFFUSIONS
- DIFFUSIONS (1,N) — COMPORTE — (1,1) OBSERVATIONS
- GAMES (0,N) — CONCERNE — (0,1) OBSERVATIONS

Exemple réel d'un objet renvoyé par `/helix/streams` (pour référence des champs
disponibles) :
```json
{
  "id": "318293462629",
  "user_id": "31289086",
  "user_login": "wankilstudio",
  "user_name": "WankilStudio",
  "game_id": "502317",
  "game_name": "60 Parsecs!",
  "type": "live",
  "title": "60 SECONDES POUR SAUVER LA GALAXIE | !wankul !holy",
  "viewer_count": 7959,
  "started_at": "2026-08-19T18:58:14Z",
  "language": "fr",
  "thumbnail_url": "https://static-cdn.jtvnw.net/previews-ttv/live_user_wankilstudio-{width}x{height}.jpg",
  "tag_ids": [],
  "tags": ["Français"],
  "is_mature": false
}
```

## Décisions d'implémentation (prises pendant le codage, pas déductibles du MLD)

### MPD — `db/schema.sql`

- **Dates en TEXT ISO-8601 UTC** (pas INTEGER epoch) : c'est le format natif de l'API
  (`"2026-08-19T18:58:14Z"`), donc zéro conversion au chargement, et compatible avec
  `date()`/`strftime()`/`julianday()` pour la heatmap heure × jour.
- **Tables `STRICT`** activées (SQLite 3.45 en local) : typage réel au lieu de
  l'affinité laxiste — les erreurs de type remontent tôt, ce qui est le but sur un
  projet d'apprentissage.
- **`NOT NULL` explicite sur toutes les PK textuelles** : SQLite laisse passer des NULL
  dans une `PRIMARY KEY` (bug historique conservé pour compatibilité), sauf pour
  `INTEGER PRIMARY KEY` et les tables `WITHOUT ROWID`. Vérifié en pratique.
- **`observations.id` = `INTEGER PRIMARY KEY` sans `AUTOINCREMENT`** : l'alias de rowid
  auto-incrémente déjà ; `AUTOINCREMENT` n'ajouterait qu'une garantie de
  non-réutilisation d'id après suppression, inutile ici.
- **`UNIQUE (diffusion_id, captured_at)` sur `observations`** : c'est ce qui rend le
  chargement des faits idempotent (permet `INSERT OR IGNORE` partout, une seule
  stratégie au lieu de deux).
- **`COLLATE NOCASE` sur `tags.tag_name`** : évite que "Français" et "français" fassent
  deux lignes distinctes.
- **`ON DELETE CASCADE` uniquement sur `observation_tags.observation_id`** : pas de
  cascade en amont (diffusions/streamers/games) pour éviter des suppressions en chaîne
  non voulues sur les données de faits.
- **Index créés explicitement sur les colonnes de FK** : contrairement à MySQL, SQLite
  ne les crée pas automatiquement.
- **`PRAGMA foreign_keys = ON` est à rejouer à chaque connexion** — ne se stocke pas
  dans le fichier `.db`. Fait dans `get_connection()`.

### `transform.py`

- Patron commun aux fonctions `extract_*` : **dict indexé par la clé de dédup**, puis
  `list(d.values())`. La clé du dict EST la clé de déduplication (`game_id` pour les
  jeux, `user_id` pour les streamers, le tag lui-même pour les tags — surtout pas
  `stream['id']`, qui est l'id de diffusion).
- `_clean_game_id()` : petite fonction utilitaire qui convertit `""` en `None`,
  utilisée par `build_observations`. Volontairement PAS branchée dans `extract_games`
  (qui garde son `if stream['game_id']:` en dur) — arbitrage assumé entre centralisation
  de la règle et lisibilité locale, à revoir si la règle se complexifie.
- `build_observation_tags(streams, observation_ids)` prend un mapping
  `{diffusion_id: observation_id}` en paramètre : elle ne peut pas s'exécuter au même
  moment que les autres, car les ids d'observations sont générés par SQLite à
  l'insertion. C'est `load.py` qui construit ce mapping et l'appelle.
- Un `seen = set()` y protège contre un tag répété dans un même `stream['tags']`, qui
  violerait la PK composite.

### `load.py`

- **Ordre d'insertion imposé par les FK** : `games`/`streamers`/`tags` (aucune
  dépendance) → `diffusions` (dépend de streamers) → `observations` (dépend de
  diffusions + games) → `observation_tags` (dépend du mapping renvoyé par
  `load_observations`).
- `load_observations` **n'utilise pas `executemany`** : insertion ligne par ligne pour
  récupérer l'id généré. Piège important — avec `INSERT OR IGNORE`, si la ligne existe
  déjà, `cursor.lastrowid` ne pointe pas dessus (il garde l'ancienne valeur). D'où un
  `SELECT id ... WHERE diffusion_id = ? AND captured_at = ?` après chaque insert, qui
  marche que la ligne soit neuve ou pas. C'est ce qui rend la relance vraiment idempotente.
- `load_transformed(streams, transformed)` prend **deux** paramètres : `streams` (la
  liste brute) est nécessaire en plus du dict transformé, à cause de
  `build_observation_tags`.
- **Idempotence vérifiée** : deux exécutions successives sur le même fichier donnent
  des compteurs identiques (149 observations, pas de doublon).

### Observations sur les données réelles

- **L'API renvoie parfois le même `id` de diffusion deux fois dans un même snapshot**
  (4 cas sur un fichier de 150 streams). La dédup dans `extract_diffusions` absorbe ça —
  ce n'était pas anticipé au moment d'écrire le MLD.
- Environ 1 stream sur 150 a un `game_id` vide → `NULL` en base.
- Ordre de grandeur par snapshot : 150 streams → ~55 jeux, ~149 streamers, ~370 tags,
  ~660 lignes de liaison.

## Ce que les données ont révélé (26/08/2026)

Résultats des 12 outils de `sql/analyses/`. Les chiffres sont ceux de la collecte
complète (130 280 observations).

- **Le tag `tags` peut être `null`** dans la réponse API (pas seulement `[]`). 50 des
  882 fichiers plantaient entièrement à cause de ça. Corrigé par `stream['tags'] or []`
  dans `extract_tags` et `build_observation_tags`.
- **Quatre variantes de « Français » coexistent** : la forme courante (128 088 usages),
  `francais` sans accent (4 622), `Francąis` avec un *a-ogonek* polonais à la place du
  *c-cédille* (229), et une quatrième **visuellement identique à la première** encodée
  en NFD (`c` + cédille combinante, 1 usage). Au total **43 groupes de tags** à
  fusionner. `COLLATE NOCASE` ne replie que la casse ASCII — la normalisation
  (minuscules + suppression des accents via `unicodedata` NFD) doit se faire côté
  Python. Voir `normaliser_tag()` dans `src/analyses.py`.
- **12 observations à 0 viewer** : `MIN()` est donc fragile pour le seuil d'entrée.
  Les analyses utilisent la médiane.
- **Piège statistique évité sur les changements de jeu** : en moyenne des pourcentages,
  changer de jeu semblait rapporter **+7,8 %**. En médiane, sur les streams d'au moins
  30 viewers, c'est **−1,0 %** — signe inverse. Un stream passant de 10 à 20 viewers
  pèse +100 % et écrase la moyenne.
- **Seuil pour la phase 2 (Option B)** : à `COUNT(*) >= 100` observations, on garde
  **397 streamers** (15 % du total) qui concentrent **72,9 %** de l'audience.
- **Courbe de vie d'un live** : montée pendant 2 h, pic, puis érosion lente — 64 % du
  pic après 10 h de direct.
- **Seuil d'entrée dans le top 150** : 12 viewers à 7h, 136 à minuit (heure de Paris).
  Le classement ne mesure pas la même chose selon l'heure.
- **L'API renvoie parfois le même id de diffusion deux fois** dans un seul snapshot.

## Prochaines étapes (dans l'ordre)

**Phase 1 — Base de données & SQL**
1. ~~Écrire `db/schema.sql` (MPD)~~ — FAIT
2. ~~Compléter `transform.py`~~ — FAIT
3. ~~Compléter `load.py`~~ — FAIT
4. ~~Charger tous les fichiers de `data/raw/`~~ — FAIT (`load_all_snapshots`)
5. ~~Construire une heatmap (heure × jour)~~ — FAIT (artefact publié)
6. ~~Développer les outils d'analyse~~ — FAIT (12 outils dans `sql/analyses/`)
7. Écrire les requêtes SQL en progression dans `sql/queries/` (bases → agrégations →
   jointures → CTE → window functions) — `01_bases.sql` fait, 02 à 05 à faire
8. Appliquer la normalisation des tags en amont du chargement (43 groupes à fusionner)
   — décision à prendre : corriger dans `transform.py`, ou garder une table de
   synonymes séparée pour ne pas perdre la donnée brute

**Phase 2 — Dataset ML & modélisation** (après la phase 1)
9. Extraction du dataset ML via SQL, deux scopes distincts à traiter séparément :
   - Option A : agrégé par jeu (GROUP BY jeu/heure/jour) — dataset robuste
   - Option B : restreint aux streamers réguliers (seuil de présence à déterminer via
     `GROUP BY user_id HAVING COUNT(*) > seuil`) — un seul modèle pooled avec le
     streamer en feature catégorielle, pas un modèle par streamer
10. Entraînement des deux modèles (régression classique ou gradient boosting léger),
   split chronologique train/test (jamais aléatoire — fuite d'information sinon)
11. Présentation : comparaison Option A vs B et de leurs limites (biais du top streams —
   les streamers moyens n'apparaissent que lors de leurs pics, à documenter comme
   limite du dataset)

## Suivi du projet

Le détail de l'avancement, des difficultés rencontrées et des décisions prises est
tenu dans une page Notion dédiée (carnet de bord + journal d'apprentissage). Non
consultable depuis ce contexte, mais mentionne-le si une décision semble contredire
ce document — il fait foi en cas de divergence.
