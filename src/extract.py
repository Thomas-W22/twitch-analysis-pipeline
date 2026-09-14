"""
Extraction — récupère le top streams Twitch et sauvegarde le JSON brut horodaté.

Ce module ne transforme rien : il capture la réponse brute de l'API telle quelle,
dans data/raw/, pour garder une trace fidèle de ce qui a été reçu à chaque instant.

Le périmètre est restreint au Twitch francophone via le paramètre `language=fr`
de l'API Helix.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import time
import requests

from src.auth import get_access_token, get_auth_headers

STREAMS_URL = "https://api.twitch.tv/helix/streams"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
TARGET_STREAM_COUNT = 150  # top ~150 streams par capture
STREAM_LANGUAGE = "fr"  # code ISO 639-1 : ne garde que le Twitch francophone


def fetch_top_streams(
    headers: dict,
    target_count: int = TARGET_STREAM_COUNT,
    language: str = STREAM_LANGUAGE,
) -> list[dict]:
    """Récupère les `target_count` premiers streams, filtrés par langue.

    Le filtre `language` est appliqué côté API : Twitch ne renvoie que les streams
    dont la langue déclarée correspond, et le classement par viewers décroissant
    porte donc bien sur le top francophone (et non sur un sous-ensemble du top monde).

    Si moins de `target_count` streams sont en ligne, renvoie tous ceux disponibles.
    """
    streams = []
    cursor = None

    while len(streams) < target_count:
        params = {
            "first": min(100, target_count - len(streams)),
            "language": language,
        }
        if cursor:
            params["after"] = cursor

        resp = requests.get(STREAMS_URL, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        batch = data["data"]
        streams.extend(batch)
        cursor = data.get("pagination", {}).get("cursor")

        # On s'arrête dès que Twitch n'a plus rien à donner : soit plus de curseur,
        # soit une page vide (l'API renvoie parfois un curseur en fin de liste).
        # Aux heures creuses, il y a moins de 150 streams FR actifs : on garde
        # simplement tous ceux qui sont en cours.
        if not cursor or not batch:
            break

    return streams[:target_count]


def save_raw_snapshot(streams: list[dict]) -> Path:
    """Sauvegarde la liste de streams dans un fichier JSON horodaté sous data/raw/.

    Nom de fichier : streams_YYYYMMDD_HHMMSS.json (UTC)
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DIR / f"streams_{timestamp}.json"

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "language": STREAM_LANGUAGE,
        "count": len(streams),
        "streams": streams,
    }
    filepath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return filepath


def run_once() -> None:
    """Effectue une capture unique : auth, fetch, sauvegarde."""
    token = get_access_token()
    headers = get_auth_headers(token)
    streams = fetch_top_streams(headers)
    filepath = save_raw_snapshot(streams)
    print(f"[{datetime.now(timezone.utc).isoformat()}] {len(streams)} streams sauvegardés → {filepath.name}")



def run_polling_loop(interval_minutes: int = 10) -> None:
    token = get_access_token()
    headers = get_auth_headers(token)
    token_obtained_at = time.time()

    while True:
        try:
            # Renouvelle le token s'il a plus de 50 jours (marge de sécurité)
            if time.time() - token_obtained_at > 50 * 24 * 3600:
                token = get_access_token()
                headers = get_auth_headers(token)
                token_obtained_at = time.time()

            streams = fetch_top_streams(headers)
            filepath = save_raw_snapshot(streams)
            print(f"[{datetime.now(timezone.utc).isoformat()}] "
                  f"{len(streams)} streams sauvegardés → {filepath.name}")

        except requests.exceptions.RequestException as e:
            print(f"Erreur réseau, on continue : {e}")

        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    run_polling_loop(interval_minutes=10)
