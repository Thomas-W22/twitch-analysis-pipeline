"""
Transform — nettoie et normalise les snapshots JSON bruts en lignes prêtes à charger.

Objectif : transformer un fichier data/raw/streams_*.json en un format qui colle
au modèle relationnel défini dans db/schema.sql (games, streamers, diffusions,
observations, tags, observation_tags).
"""
import json
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def load_raw_snapshot(filepath: Path) -> dict:
    """Charge un fichier JSON brut sauvegardé par extract.py."""
    return json.loads(filepath.read_text(encoding="utf-8"))


def extract_games(streams: list[dict]) -> list[dict]:
    """Extrait la liste dédupliquée des jeux présents dans ce snapshot.
    - Chaque stream a un game_id et game_name
    - Attention aux streams sans jeu associé (game_id vide) — à gérer ou filtrer
    - Retourne une liste de dicts uniques {"game_id": ..., "name": ...}
    """
    d={}
    for stream in streams : 
        if stream['game_id']:
            d[stream['game_id']]= {"game_id" : stream['game_id'] , "name" : stream['game_name']}
    return list(d.values())

    

def extract_streamers(streams: list[dict]) -> list[dict]:
    """Extrait la liste dédupliquée des streamers présents dans ce snapshot.
    - Chaque stream a un user_id, user_login, user_name
    - Retourne une liste de dicts uniques {"user_id": ..., "login": ..., "display_name": ...}
    """
    d = {}
    for stream in streams : 
        d[stream['user_id']] = {"user_id" : stream['user_id'], "login" : stream['user_login'], 
                           "display_name" : stream['user_name']}
    return list(d.values())


def extract_tags(streams: list[dict]) -> list[dict]:
    """Extrait la liste dédupliquée des tags présents dans ce snapshot.
    - `tags` est une LISTE par stream (ex: ["Français"]), pas un scalaire —
      il faut donc une boucle dans la boucle (ou une compréhension imbriquée)
      pour atteindre chaque tag individuel.
    - Retourne une liste de dicts uniques {"tag_name": ...}
    - La table tags a COLLATE NOCASE en base : tu peux dédupliquer tel quel en
      Python et laisser SQLite gérer la casse au INSERT OR IGNORE, ou
      normaliser toi-même ici. À toi de choisir.
    """
    d = {}
    for stream in streams:
        # L'API renvoie parfois tags: null (pas [] ) -> `or []` évite le TypeError
        for tag in stream['tags'] or []:
            d[tag] = {'tag_name': tag}
    return list(d.values())


def _clean_game_id(raw_game_id: str) -> str | None:
    """Convertit un game_id vide ("") en None — patte (0,1) de CONCERNE.

    Factorisée ici car utilisée à la fois par extract_games et
    build_observations : éviter d'écrire la même règle "" -> None deux fois.
    """
    return raw_game_id if raw_game_id else None


def extract_diffusions(streams: list[dict]) -> list[dict]:
    """Extrait la liste dédupliquée des diffusions présentes dans ce snapshot."""
    d = {}
    for stream in streams:
        d[stream['id']] = {
            "id": stream['id'],
            "started_at": stream['started_at'],
            "user_id": stream['user_id'],
        }
    return list(d.values())


def build_observations(streams: list[dict], captured_at: str) -> list[dict]:
    """Construit les lignes de la table de faits observations.

    Une ligne PAR STREAM (pas de dict/dédup ici : chaque snapshot est un fait
    distinct, même valeur de captured_at pour toutes les lignes du fichier).
    """
    observations = []
    for stream in streams:
        observations.append({
            "viewer_count": int(stream['viewer_count']),
            "title": stream['title'],
            "language": stream['language'],
            "captured_at": captured_at,
            "diffusion_id": stream['id'],
            "game_id": _clean_game_id(stream['game_id']),
        })
    return observations


def build_observation_tags(streams: list[dict], observation_ids: dict) -> list[dict]:
    """Construit les lignes de la table de liaison observation_tags.

    observation_ids : dict {diffusion_id: observation_id}, construit par
    load.py APRÈS l'insertion des observations (diffusion_id = stream['id']
    identifie une observation de façon unique DANS UN SEUL FICHIER, puisque
    build_observations en crée une ligne par stream — un stream = une
    diffusion par snapshot). load.py récupère l'id auto-incrémenté généré par
    SQLite via cursor.lastrowid à chaque INSERT et construit ce mapping.
    """
    observation_tags = []
    seen = set()  # évite les doublons (observation_id, tag_name) si un tag
                  # apparaît plusieurs fois dans stream['tags']
    for stream in streams:
        observation_id = observation_ids[stream['id']]
        for tag in stream['tags'] or []:  # tags peut être null côté API
            pair = (observation_id, tag)
            if pair not in seen:
                seen.add(pair)
                observation_tags.append({
                    "observation_id": observation_id,
                    "tag_name": tag,
                })
    return observation_tags


def transform_snapshot(filepath: Path) -> dict:
    """Pipeline complet de transformation pour un fichier brut donné.

    Returns:
        dict avec les clés "games", "streamers", "diffusions", "tags",
        "observations" — chacune une liste de dicts prête à être chargée
        dans SQLite. observation_tags n'est PAS construite ici (voir
        build_observation_tags) : elle dépendra des ids générés par load.py.
    """
    raw = load_raw_snapshot(filepath)
    streams = raw["streams"]
    captured_at = raw["captured_at"]

    return {
        "games": extract_games(streams),
        "streamers": extract_streamers(streams),
        "diffusions": extract_diffusions(streams),
        "tags": extract_tags(streams),
        "observations": build_observations(streams, captured_at),
    }


if __name__ == "__main__":
    # Test rapide sur le fichier brut le plus récent
    files = sorted(RAW_DIR.glob("streams_*.json"))
    if not files:
        print("Aucun fichier brut trouvé — lance d'abord src/extract.py")
    else:
        result = transform_snapshot(files[-1])
        print(f"{len(result['games'])} jeux, {len(result['streamers'])} streamers, "
              f"{len(result['diffusions'])} diffusions, {len(result['tags'])} tags, "
              f"{len(result['observations'])} observations")
