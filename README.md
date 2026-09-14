# Twitch SQL — Pipeline ETL & Analyse SQL

Projet personnel de pratique de la gestion de données : un pipeline ETL collecte en direct
le top des streams Twitch via l'API Helix, les stocke dans SQLite, et sert de terrain
d'entraînement SQL — des requêtes de base jusqu'aux window functions — puis de dataset pour
une phase de modélisation.

L'intérêt n'est pas le volume : c'est de travailler sur des **données réelles, imparfaites et
temporelles**, collectées par mes soins plutôt que téléchargées propres depuis Kaggle. Les
pièges rencontrés (doublons dans un même appel API, quatre orthographes du tag « Français »,
moyennes trompeuses) font partie de l'exercice.

---

## Sommaire

- [Portée du projet](#portée-du-projet)
- [Architecture](#architecture)
- [Choix techniques](#choix-techniques)
- [Modèle de données](#modèle-de-données)
- [Structure du dépôt](#structure-du-dépôt)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Ce qu'on peut faire avec les données](#ce-quon-peut-faire-avec-les-données)
- [Ce que les données ont révélé](#ce-que-les-données-ont-révélé)
- [Limites connues](#limites-connues)
- [État d'avancement](#état-davancement)

---

## Portée du projet

| Dimension | Choix retenu |
|---|---|
| **Source** | API Twitch Helix, endpoint `/helix/streams`, en direct |
| **Authentification** | Client Credentials flow (app-only, pas d'auth utilisateur) |
| **Périmètre** | Top ~150 streams par `viewer_count`, déjà triés par l'API |
| **Fréquence** | Une capture toutes les 10 minutes |
| **Durée** | Collecte continue depuis le 19 août 2026 |
| **Stockage** | JSON brut horodaté en staging, puis SQLite |

Le périmètre « top 150 » est une contrainte assumée : l'API ne permet pas de balayer les
~100 000 streams simultanés. On observe donc la tête du classement, ce qui suffit largement
pour étudier des dynamiques temporelles — mais oriente les conclusions (voir
[Limites connues](#limites-connues)).

**Hors périmètre, volontairement** : les endpoints `/helix/users`, `/helix/videos` et
`/helix/games/top` ; les champs `is_mature`, `thumbnail_url` et `type` ; le chat et les
événements EventSub.

---

## Architecture

```
                  ┌──────────────────────┐
                  │  API Twitch Helix    │
                  │  /helix/streams      │
                  └──────────┬───────────┘
                             │  toutes les 10 min, pagination 100/page
                             ▼
    ┌────────────────────────────────────────────┐
    │  EXTRACT — src/extract.py                  │
    │  boucle de polling + sauvegarde brute      │
    └────────────────────┬───────────────────────┘
                         ▼
              data/raw/streams_YYYYMMDD_HHMMSS.json      ← staging immuable
                         │
                         ▼
    ┌────────────────────────────────────────────┐
    │  TRANSFORM — src/transform.py              │
    │  un JSON plat  →  6 collections dédupées   │
    └────────────────────┬───────────────────────┘
                         ▼
    ┌────────────────────────────────────────────┐
    │  LOAD — src/load.py                        │
    │  chargement idempotent, ordre imposé par   │
    │  les clés étrangères                       │
    └────────────────────┬───────────────────────┘
                         ▼
              data/processed/twitch.db               ← SQLite, 6 tables
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
    sql/queries/                sql/analyses/
    exercices SQL               12 outils d'analyse
    (progression)               (src/analyses.py)
                                        │
                                        ▼
                             heatmap.html, dashboard.html
```

### Pourquoi cette séparation en trois étages

**Le staging JSON brut est le point le plus important du design.** L'API ne renvoie que
l'instant présent : une capture manquée est perdue pour toujours. Écrire d'abord le JSON tel
quel sur disque garantit que la collecte ne dépend jamais de l'état du schéma, du code de
transformation ou de la base. Cela s'est vérifié en pratique : 50 fichiers plantaient au
chargement à cause d'un `tags: null` inattendu — le correctif appliqué, il a suffi de
relancer `load_all_snapshots()` sur les fichiers déjà sur disque. Sans staging, ces
50 captures auraient disparu.

Le chargement est **idempotent** de bout en bout : `INSERT OR IGNORE` partout, adossé à une
contrainte `UNIQUE (diffusion_id, captured_at)` sur la table de faits. Relancer le
chargement complet ne crée jamais de doublon, ce qui permet de rejouer librement après
correction.

---

## Choix techniques

### SQLite plutôt que PostgreSQL

Le volume (quelques centaines de milliers de lignes) tient très largement dans SQLite, et
l'objectif est la pratique SQL, pas l'administration d'un serveur. Une base = un fichier :
sauvegarde triviale, aucune installation, portable. Les fonctionnalités réellement utilisées
ici (CTE, window functions, `strftime`) sont toutes présentes.

### Pas d'orchestrateur

Airflow, Prefect ou Dagster seraient disproportionnés pour une tâche unique déclenchée
toutes les 10 minutes. Une boucle `while True` + `time.sleep()` dans `run_polling_loop()`
suffit, avec renouvellement du token au-delà de 50 jours et un `try/except` réseau qui
laisse la boucle survivre à une coupure passagère. Le script tourne sur un poste avec la
mise en veille désactivée.

### Deux dépendances seulement

`requests` et `python-dotenv`. Ni pandas ni SQLAlchemy : toute la logique d'analyse est
écrite en SQL — c'est précisément l'objet du projet. Python ne fait qu'orchestrer et
afficher.

### Dates en TEXT ISO-8601 UTC

Format natif de l'API (`"2026-08-19T18:58:14Z"`), donc **zéro conversion au chargement** —
une source d'erreurs en moins. SQLite sait manipuler ce format avec `date()`, `strftime()`
et `julianday()`, ce qui couvre tous les besoins d'analyse temporelle (heatmap heure × jour,
ancienneté d'un live, agrégats journaliers).

### Tables `STRICT`

Activées sur les 6 tables. SQLite applique par défaut une « affinité » de type très
permissive (une chaîne peut atterrir dans une colonne `INTEGER`). Sur un projet
d'apprentissage, on veut au contraire que les erreurs de type remontent tôt et bruyamment.

### Le SQL vit dans des fichiers `.sql`

Chaque outil d'analyse est un fichier autonome et commenté dans `sql/analyses/`.
`src/analyses.py` ne fait que les exécuter et formater le résultat. Le SQL reste lisible,
versionné et exécutable tel quel dans n'importe quel client — il n'est jamais noyé dans des
chaînes Python.

---

## Modèle de données

Six entités, issues d'un MCD → MLD → MPD (voir `MCD.drawio` et `db/schema.sql`).

```
  STREAMERS ──(1,N) RÉALISE (1,1)──> DIFFUSIONS ──(1,N) COMPORTE (1,1)──> OBSERVATIONS
                                                                              │
                                          GAMES <──(0,N) CONCERNE (0,1)───────┤
                                                                              │
                                          TAGS <──(0,N) OBSERVATION_TAGS (0,N)┘
```

| Table | Rôle | Clé |
|---|---|---|
| `streamers` | Dimension — identité du diffuseur | `user_id` (naturelle, API) |
| `diffusions` | Une session de live | `id` (naturelle, stream id de l'API) |
| `observations` | **Table de faits** — une photo d'une diffusion à un instant | `id` (technique) |
| `games` | Dimension — le jeu diffusé | `game_id` (naturelle, API) |
| `tags` | Dimension — libellé de tag | `tag_name` (naturelle) |
| `observation_tags` | Liaison observations ↔ tags | PK composite |

### La décision structurante : diffusions vs observations

`diffusions` ne contient **que ce qui ne change pas** pendant une session de live
(`started_at`, le streamer). Tout ce qui peut varier — `viewer_count`, `title`, `language`,
le jeu — vit dans `observations`. Un streamer change généralement de titre et de jeu
ensemble, au même moment : les deux appartiennent donc au même niveau de granularité.

C'est ce découpage qui rend possible toute l'analyse temporelle : suivre l'audience minute
par minute, détecter un changement de jeu entre deux captures consécutives, calculer
l'ancienneté d'un live au moment de son pic.

### Autres décisions de schéma

- **Clés naturelles partout où l'API en fournit une.** Une seule clé technique dans tout le
  schéma : `observations.id`. Pas de clé de substitution inventée là où Twitch en donne déjà
  une stable.
- **`UNIQUE (diffusion_id, captured_at)`** — la contrainte qui porte toute l'idempotence.
- **`COLLATE NOCASE` sur `tags.tag_name`** — évite que « Français » et « français » créent
  deux lignes. (Ne replie que la casse ASCII : la normalisation des accents se fait côté
  Python, voir [Limites connues](#limites-connues).)
- **`NOT NULL` explicite sur toutes les PK textuelles** — SQLite laisse passer des `NULL`
  dans une `PRIMARY KEY` (bug historique conservé pour compatibilité).
- **Index créés à la main sur les colonnes de FK** — contrairement à MySQL, SQLite ne les
  crée pas automatiquement ; sans eux, chaque jointure fait un scan complet.
- **`ON DELETE CASCADE` uniquement sur `observation_tags.observation_id`** — pas de cascade
  en amont, pour qu'une suppression de dimension n'emporte jamais des faits.
- **`PRAGMA foreign_keys = ON` rejoué à chaque connexion** — ce réglage ne se stocke pas
  dans le fichier `.db`. Fait dans `get_connection()`.

---

## Structure du dépôt

```
2_Twitch_Analysis/
├── src/
│   ├── auth.py           # Client Credentials flow → token + headers
│   ├── extract.py        # fetch_top_streams (pagination) + run_polling_loop
│   ├── transform.py      # JSON brut → 6 collections dédupées
│   ├── load.py           # chargement idempotent + load_all_snapshots()
│   └── analyses.py       # exécute les .sql de sql/analyses/
├── db/
│   └── schema.sql        # MPD : 6 tables STRICT + index, entièrement commenté
├── sql/
│   ├── analyses/         # 12 outils d'analyse, un .sql commenté par outil
│   └── queries/          # exercices SQL progressifs (01_bases → 05_window_functions)
├── data/
│   ├── raw/              # JSON bruts horodatés (staging, gitignored)
│   └── processed/        # twitch.db (généré, gitignored)
├── notebooks/            # exploration ponctuelle
├── MCD.drawio            # modèle conceptuel de données
├── CONTEXTE_PROJET.md    # journal des décisions techniques
├── heatmap.html          # artefact — heatmap heure × jour
├── dashboard.html        # artefact — les 12 analyses
├── .env.example
└── requirements.txt
```

---

## Installation

```bash
python -m venv venv
venv\Scripts\activate        # Windows  (source venv/bin/activate sur Linux/macOS)
pip install -r requirements.txt
cp .env.example .env         # puis renseigner TWITCH_CLIENT_ID et TWITCH_CLIENT_SECRET
```

Les identifiants s'obtiennent en enregistrant une application sur la
[console développeur Twitch](https://dev.twitch.tv/console/apps). Le flow Client Credentials
ne demande aucune autorisation d'utilisateur : il donne accès aux données publiques, ce qui
est exactement le périmètre ici.

Initialisation de la base :

```bash
sqlite3 data/processed/twitch.db < db/schema.sql
```

---

## Utilisation

**Collecte** — une capture unique, ou la boucle continue :

```bash
python -c "from src.extract import run_once; run_once()"
python -m src.extract           # boucle toutes les 10 minutes, Ctrl+C pour arrêter
```

**Chargement** — tous les fichiers de `data/raw/` en une passe (relançable sans risque) :

```bash
python -m src.load
```

Une seule connexion pour tous les fichiers, commit tous les 25, et un `try/except` par
fichier : un JSON corrompu n'interrompt pas le traitement des autres.

**Analyses** — les 12 outils, ou une sélection :

```bash
python -m src.analyses          # tout
python -m src.analyses 01 07 11 # seulement ces outils
```

---

## Ce qu'on peut faire avec les données

Le jeu de données est une **série temporelle à pas de 10 minutes** sur la tête du classement
Twitch. Chaque diffusion y apparaît comme une suite d'observations horodatées, ce qui ouvre
plusieurs familles d'analyses.

### Les 12 outils déjà écrits (`sql/analyses/`)

| # | Outil | Question |
|---|---|---|
| 01 | Changements de jeu | Changer de jeu en cours de live fait-il gagner ou perdre de l'audience ? |
| 02 | Changements de titre | Qui édite son titre en direct (marathons, streams 24h) ? |
| 03 | Cycle de vie | Quelle est la courbe d'audience type d'un live, du début à la fin ? |
| 04 | Rediffusions | Isoler les replays, qui faussent toute mesure de « direct » |
| 05 | Seuil d'entrée | Combien de viewers faut-il pour entrer dans le top 150, heure par heure ? |
| 06 | Rotation du top | Quelle part du top est renouvelée chaque jour ? |
| 07 | Régularité des streamers | Combien de streamers réguliers couvrent l'essentiel de l'audience ? |
| 08 | Pics d'audience | Détecter raids, events et redirections par les bonds anormaux |
| 09 | Parts d'audience par jeu | Repérer les hypes (sorties, events — la collecte couvre la Gamescom) |
| 10 | Prime time par jeu | Chaque jeu a-t-il sa propre forme horaire d'audience ? |
| 11 | Normalisation des tags | **Qualité de données** — recenser les variantes d'un même tag |
| 12 | Drops activés | Le tag `DropsActives` corrèle-t-il avec une audience supérieure ? |

Ces outils mobilisent l'essentiel du SQL analytique : `LAG`/`LEAD` pour comparer deux
captures consécutives, `PARTITION BY` pour normaliser par diffusion, CTE empilées,
agrégats conditionnels, jointures réflexives.

### Autres pistes ouvertes par le modèle

- **Analyse de cohortes** — suivre dans le temps les streamers apparus une semaine donnée.
- **Co-occurrence de tags** — auto-jointure sur `observation_tags` pour cartographier les
  familles de contenus.
- **Saisonnalité fine** — le pas de 10 minutes permet de distinguer un pic de prime time
  d'un raid ponctuel.
- **Détection de sessions anormales** — écarts à la courbe de vie type de l'outil 03.
- **Cartographie linguistique** — `language` croisé avec les heures : chaque communauté a
  son prime time.

### Phase 2 — dataset de modélisation

Deux périmètres, à traiter séparément :

- **Option A — agrégé par jeu** (`GROUP BY` jeu × heure × jour). Dataset robuste, beaucoup
  d'observations par ligne, peu de bruit.
- **Option B — streamers réguliers.** L'outil 07 a fixé le seuil : à `COUNT(*) >= 100`
  observations, on retient **397 streamers (15 % du total) qui concentrent 72,9 % de
  l'audience**. Un seul modèle *pooled* avec le streamer en variable catégorielle, pas un
  modèle par streamer.

Dans les deux cas, le split train/test doit être **chronologique**, jamais aléatoire : un
split aléatoire mettrait la capture de 14h10 en test et celles de 14h00 et 14h20 en train,
ce qui revient à donner la réponse au modèle.

---

## Ce que les données ont révélé

Résultats sur la collecte chargée au 26/08/2026 — **130 280 observations**, 7 272 diffusions,
2 606 streamers, 742 jeux, 605 672 liaisons de tags, sur 6 j 16 h.

- **Un piège statistique évité.** En moyenne des pourcentages, changer de jeu semblait
  rapporter **+7,8 %**. En médiane, sur les streams d'au moins 30 viewers, c'est **−1,0 %** —
  signe inverse. Un stream passant de 10 à 20 viewers pèse +100 % et écrase la moyenne. Les
  analyses utilisent désormais la médiane, d'autant que 12 observations sont à 0 viewer.
- **Le classement ne mesure pas la même chose selon l'heure.** Le seuil d'entrée dans le
  top 150 va de **12 viewers à 7h** à **136 à minuit** (heure de Paris). Comparer une
  position à deux heures différentes n'a pas de sens sans normalisation.
- **Courbe de vie d'un live** : montée pendant environ 2 h, pic, puis érosion lente —
  encore **64 % du pic après 10 h** de direct.
- **43 groupes de tags à fusionner.** Quatre variantes de « Français » coexistent : la forme
  courante (128 088 usages), `francais` sans accent (4 622), `Francąis` avec un *a-ogonek*
  polonais à la place du *c-cédille* (229), et une quatrième **visuellement identique à la
  première**, encodée en NFD (`c` + cédille combinante, 1 usage).
- **L'API n'est pas propre.** Elle renvoie parfois le même id de diffusion **deux fois dans
  un même snapshot** (4 cas sur un fichier de 150). Le champ `tags` peut valoir `null` et pas
  seulement `[]` — ce qui faisait planter 50 fichiers sur 882. Environ 1 stream sur 150 a un
  `game_id` vide, converti en `NULL`.

---

## Limites connues

- **Biais de sélection du top 150.** Un streamer moyen n'entre dans les données que lors de
  ses pics. Toute statistique « par streamer » est donc conditionnée à la présence dans le
  top — à documenter systématiquement dans les conclusions.
- **Normalisation des tags non appliquée en amont.** Les 43 groupes de variantes sont
  identifiés (outil 11) mais la base stocke encore les libellés bruts. Décision en suspens :
  corriger dans `transform.py`, ou maintenir une table de synonymes pour ne pas perdre la
  donnée d'origine.
- **Collecte dépendante d'un poste local.** Pas de redondance : une coupure de courant crée
  un trou dans la série. Les trous sont visibles dans `data/raw/` (fichiers manquants) et
  détectables en SQL sur `captured_at`.
- **`broadcaster_type` absent.** Ce champ n'existe que sur `/helix/users`, endpoint hors
  périmètre — impossible de distinguer partenaire et affilié.
- **Pas de données de chat ni de revenus.** L'audience est mesurée par le seul
  `viewer_count`.

---

## État d'avancement

**Fait**

- MCD, MLD et MPD finalisés (`db/schema.sql`)
- Pipeline ETL fonctionnel de bout en bout, idempotent et relançable
- Collecte en continu depuis le 19 août 2026 — **2 568 snapshots** sur disque
- Base chargée jusqu'au 26/08/2026 : 882 fichiers, 0 erreur, 130 280 observations
- 12 outils d'analyse SQL + le runner `src/analyses.py`
- Deux artefacts publiés : heatmap heure × jour, et tableau de bord des 12 analyses

**En cours / à faire — phase 1**

- Recharger les snapshots collectés depuis le 26/08 (`python -m src.load`)
- Exercices SQL de `sql/queries/` : `01_bases.sql` est écrit (exemple résolu + 7 exercices),
  les niveaux 02 à 05 restent à remplir
- Trancher la normalisation des tags (43 groupes)

**Phase 2 — modélisation**

- Extraction du dataset ML en SQL, options A et B
- Entraînement des deux modèles (régression ou gradient boosting léger), split chronologique
- Comparaison A vs B et documentation des limites

---

Le détail de l'avancement, des difficultés rencontrées et des décisions techniques est tenu
dans un carnet de bord Notion. Les décisions déjà actées sont consignées dans
[CONTEXTE_PROJET.md](CONTEXTE_PROJET.md).
