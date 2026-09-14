"""
Authentification Twitch — Client Credentials flow.

Ce module gère l'obtention d'un App Access Token, nécessaire pour tous
les appels à l'API Helix. Ce flow ne demande pas de permission utilisateur :
il suffit du client_id et du client_secret de ton app Twitch.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TOKEN_URL = "https://id.twitch.tv/oauth2/token"


def get_access_token() -> str:
    """Récupère un App Access Token via le Client Credentials flow.

    Returns:
        Le token d'accès (str), valable ~59 jours.
    """
    resp = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_auth_headers(token: str) -> dict:
    """Construit les headers requis pour tout appel à l'API Helix."""
    return {
        "Client-Id": CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }


if __name__ == "__main__":
    # Test rapide : lance `python -m src.auth` pour vérifier que l'auth fonctionne
    token = get_access_token()
    print("Token obtenu avec succès :", token[:10] + "...")
