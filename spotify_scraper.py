import pandas as pd
import requests
import dotenv
import base64
import os

dotenv.load_dotenv()


def client_credentials_auth(client_id, client_secret, timeout=5) -> requests.Response:
    url = "https://accounts.spotify.com/api/token"
    auth_string = f"{client_id}:{client_secret}"
    b64_encoded_string = base64.b64encode(auth_string.encode("utf-8"))
    encoded = base64.b64encode(
        (client_id + ":" + client_secret).encode("ascii")
    ).decode("ascii")
    print(encoded)

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(
        url,
        params={"grant_type": "client_credentials"},
        headers=headers,
        timeout=timeout,
    )

    return response


client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

auth = client_credentials_auth(client_id, client_secret)
auth.status_code
auth.json().get("access_token")


class SpotifyExtractor:
    def __init__(self, client_id, client_secret) -> None:
        # authenticate
        dotenv.load_dotenv()
        self.client_id = client_id
        self.client_secret = client_secret

        response = self.client_credentials_auth()

        if response.ok:
            self.token = response.json().get("access_token")
        else:
            raise requests.exceptions.HTTPError(response.text)

    def client_credentials_auth(self, timeout=5) -> requests.Response:
        """"""
        url = "https://accounts.spotify.com/api/token"
        auth_string = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode((auth_string).encode("ascii")).decode("ascii")

        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = requests.post(
            url,
            params={"grant_type": "client_credentials"},
            headers=headers,
            timeout=timeout,
        )

        return response

    # actual endpoints we need to hit
    def search_playlists(self, q: str, limit: int = 50, timeout=5) -> list[dict]:
        url = "https://api.spotify.com/v1/search"
        params = {
            "q": q,
            "type": "playlist",
            "limit": limit,
        }
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(
            url=url, params=params, headers=headers, timeout=timeout
        )
        return response

    def get_playlists() -> list[dict]:
        pass

    def get_playlist_items() -> list[dict]:
        pass

    def get_track_features() -> list[dict]:
        pass

    def get_artists() -> list[dict]:
        pass

    def get_albums() -> list[dict]:
        pass

    # methods that handle iterating through the various endpoints
    def get_all_playlists() -> list[dict]:
        pass

    def get_all_songs() -> list[dict]:
        pass

    def get_all_song_features() -> list[dict]:
        pass

    def get_all_artists() -> list[dict]:
        pass

    def get_all_albums() -> list[dict]:
        pass

    # helper method for handling rate limiting, token refresh, and 404 errors
    def handle_requests() -> list[dict]:
        pass
