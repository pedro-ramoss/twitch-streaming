import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")


def get_access_token():
    url = "https://id.twitch.tv/oauth2/token"

    resposta = requests.post(
        url,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials"
        },
        timeout=15
    )

    resposta.raise_for_status()
    return resposta.json()["access_token"]


def get_streams(token):
    url = "https://api.twitch.tv/helix/streams"

    headers = {
        "Client-Id": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }

    resposta = requests.get(url, headers=headers, timeout=15)
    resposta.raise_for_status()

    return resposta.json()


def save_json(dados):
    os.makedirs("data/raw", exist_ok=True)

    with open("data/raw/twitch_streams.json", "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    token = get_access_token()
    dados = get_streams(token)

    print(json.dumps(dados, indent=4, ensure_ascii=False))
    save_json(dados)