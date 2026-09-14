"""
Load — charge les données transformées dans la base SQLite.

Le chargement doit être idempotent : relancer ce script sur les mêmes données
ne doit pas créer de doublons (utilise INSERT OR IGNORE / INSERT OR REPLACE
selon la table).

Ordre de chargement imposé par les FOREIGN KEY (voir load_transformed) :
games, streamers, tags -> diffusions -> observations -> observation_tags.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "twitch.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


def get_connection() -> sqlite3.Connection:
    """Ouvre une connexion à la base, en la créant (avec le schéma) si besoin."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new = not DB_PATH.exists()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    if is_new:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()

    return conn


def load_games(conn: sqlite3.Connection, games: list[dict]) -> None:
    """Insère les jeux, en ignorant les doublons (game_id déjà présent)."""
    conn.executemany(
        "INSERT OR IGNORE INTO games (game_id, name) VALUES (?, ?)",
        [(g["game_id"], g["name"]) for g in games],
    )


def load_streamers(conn: sqlite3.Connection, streamers: list[dict]) -> None:
    """Insère les streamers, en ignorant les doublons."""
    conn.executemany(
        "INSERT OR IGNORE INTO streamers (user_id, login, display_name) VALUES (?, ?, ?)",
        [(s["user_id"], s["login"], s["display_name"]) for s in streamers],
    )


def load_diffusions(conn: sqlite3.Connection, diffusions: list[dict]) -> None:
    """Insère les diffusions, en ignorant les doublons.

    Doit être appelée APRÈS load_streamers : diffusions.user_id référence
    streamers(user_id) via FOREIGN KEY.
    """
    conn.executemany(
        "INSERT OR IGNORE INTO diffusions (id, started_at, user_id) VALUES (?, ?, ?)",
        [(d["id"], d["started_at"], d["user_id"]) for d in diffusions],
    )


def load_tags(conn: sqlite3.Connection, tags: list[dict]) -> None:
    """Insère les tags, en ignorant les doublons."""
    conn.executemany(
        "INSERT OR IGNORE INTO tags (tag_name) VALUES (?)",
        [(t["tag_name"],) for t in tags],
    )


def load_observations(conn: sqlite3.Connection, observations: list[dict]) -> dict:
    """Insère les lignes de la table de faits observations.

    Doit être appelée après load_diffusions et load_games (FK sur les deux).

    Insertion ligne par ligne (pas d'executemany) : on a besoin de l'id
    généré par SQLite à chaque insertion pour construire le mapping attendu
    par build_observation_tags. UNIQUE (diffusion_id, captured_at) rend
    l'INSERT OR IGNORE idempotent, mais si la ligne existait déjà,
    cursor.lastrowid ne pointe pas dessus — d'où le SELECT de secours après
    chaque insert, qui fonctionne que la ligne soit neuve ou pas.

    Returns:
        dict {diffusion_id: observation_id}.
    """
    observation_ids = {}
    cur = conn.cursor()
    for obs in observations:
        cur.execute(
            """
            INSERT OR IGNORE INTO observations
                (viewer_count, title, language, captured_at, diffusion_id, game_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                obs["viewer_count"],
                obs["title"],
                obs["language"],
                obs["captured_at"],
                obs["diffusion_id"],
                obs["game_id"],
            ),
        )
        cur.execute(
            "SELECT id FROM observations WHERE diffusion_id = ? AND captured_at = ?",
            (obs["diffusion_id"], obs["captured_at"]),
        )
        observation_ids[obs["diffusion_id"]] = cur.fetchone()[0]
    return observation_ids


def load_observation_tags(conn: sqlite3.Connection, observation_tags: list[dict]) -> None:
    """Insère les lignes de la table de liaison observation_tags.

    Doit être appelée en dernier : dépend du mapping renvoyé par
    load_observations et des tags déjà chargés par load_tags.
    """
    conn.executemany(
        "INSERT OR IGNORE INTO observation_tags (observation_id, tag_name) VALUES (?, ?)",
        [(ot["observation_id"], ot["tag_name"]) for ot in observation_tags],
    )


def load_transformed(streams: list[dict], transformed: dict) -> None:
    """Pipeline complet : ouvre la connexion, charge les 6 tables dans l'ordre
    imposé par les FOREIGN KEY, commit.

    `streams` (la liste brute du JSON) est nécessaire en plus de `transformed`
    car observation_tags ne peut être construite qu'après l'insertion des
    observations (voir build_observation_tags dans transform.py, qui a besoin
    du mapping diffusion_id -> observation_id généré par SQLite).
    """
    from src.transform import build_observation_tags

    conn = get_connection()
    try:
        load_games(conn, transformed["games"])
        load_streamers(conn, transformed["streamers"])
        load_tags(conn, transformed["tags"])
        load_diffusions(conn, transformed["diffusions"])
        observation_ids = load_observations(conn, transformed["observations"])

        observation_tags = build_observation_tags(streams, observation_ids)
        load_observation_tags(conn, observation_tags)

        conn.commit()
    finally:
        conn.close()


def load_all_snapshots(verbose: bool = True) -> dict:
    """Charge TOUS les fichiers bruts de data/raw/ dans la base.

    Une seule connexion pour l'ensemble des fichiers (plutôt qu'une par
    fichier), et un commit tous les COMMIT_EVERY fichiers : c'est ce qui rend
    le chargement de plusieurs centaines de snapshots supportable en durée.

    Relançable sans risque : toutes les insertions sont idempotentes.

    Returns:
        dict de statistiques {"fichiers": n, "erreurs": n}.
    """
    from src.transform import (
        build_observation_tags,
        build_observations,
        extract_diffusions,
        extract_games,
        extract_streamers,
        extract_tags,
        load_raw_snapshot,
    )
    from src.extract import RAW_DIR

    COMMIT_EVERY = 25
    files = sorted(RAW_DIR.glob("streams_*.json"))
    stats = {"fichiers": 0, "erreurs": 0}

    conn = get_connection()
    try:
        for i, path in enumerate(files, start=1):
            try:
                raw = load_raw_snapshot(path)
                streams = raw["streams"]
                captured_at = raw["captured_at"]

                load_games(conn, extract_games(streams))
                load_streamers(conn, extract_streamers(streams))
                load_tags(conn, extract_tags(streams))
                load_diffusions(conn, extract_diffusions(streams))
                observation_ids = load_observations(
                    conn, build_observations(streams, captured_at)
                )
                load_observation_tags(
                    conn, build_observation_tags(streams, observation_ids)
                )
                stats["fichiers"] += 1
            except Exception as exc:
                # Un fichier corrompu ou tronqué (collecte interrompue) ne doit
                # pas faire échouer les 880 autres.
                stats["erreurs"] += 1
                if verbose:
                    print(f"  ERREUR sur {path.name}: {exc}")

            if i % COMMIT_EVERY == 0:
                conn.commit()
                if verbose:
                    print(f"  {i}/{len(files)} fichiers traités...", flush=True)

        conn.commit()
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    from src.transform import load_raw_snapshot, transform_snapshot
    from src.extract import RAW_DIR

    files = sorted(RAW_DIR.glob("streams_*.json"))
    if not files:
        print("Aucun fichier brut trouvé — lance d'abord src/extract.py")
    else:
        latest = files[-1]
        streams = load_raw_snapshot(latest)["streams"]
        transformed = transform_snapshot(latest)
        load_transformed(streams, transformed)
        print(f"Chargement terminé dans {DB_PATH}")
