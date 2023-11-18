import pandas as pd
import requests
import dotenv
import base64
import os

dotenv.load_dotenv()


# def client_credentials_auth(client_id, client_secret, timeout=5) -> requests.Response:
#     url = "https://accounts.spotify.com/api/token"
#     auth_string = f"{client_id}:{client_secret}"
#     b64_encoded_string = base64.b64encode(auth_string.encode("utf-8"))
#     encoded = base64.b64encode(
#         (client_id + ":" + client_secret).encode("ascii")
#     ).decode("ascii")
#     print(encoded)

#     headers = {
#         "Authorization": f"Basic {encoded}",
#         "Content-Type": "application/x-www-form-urlencoded",
#     }

#     response = requests.post(
#         url,
#         params={"grant_type": "client_credentials"},
#         headers=headers,
#         timeout=timeout,
#     )

#     return response


client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")

auth = client_credentials_auth(client_id, client_secret)
auth.status_code
token = auth.json().get("access_token")
token


# def get_playlist(playlist_id: str, timeout=5):
#     url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
#     response = requests.get(
#         url,
#         params={"fields": "tracks"},
#         headers={"Authorization": f"Bearer {token}"},
#         timeout=timeout,
#     )
#     return response


# def get_playlist_items(playlist_id: str, limit: int = 50, offset: int = 0, timeout=5):
#     url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
#     response = requests.get(
#         url,
#         params={"limit": limit, "offset": offset},
#         headers={"Authorization": f"Bearer {token}"},
#         timeout=timeout,
#     )
#     return response


# https://open.spotify.com/playlist/2a1A4HSQo3ye4wJAPM2I2l?si=b6c69c38d997487e
#  https://open.spotify.com/playlist/2brN3U7zhnFHhoKROkzfA9?si=2e92bbf3b64144bd

playlist_id = "2brN3U7zhnFHhoKROkzfA9"

playlist = get_playlist(playlist_id)
playlist.status_code
playlist.json().get("tracks").get("total")

tracks = playlist.json().get("tracks")
tracks.keys()
tracks.get("next")

tracks_response = tracks
tracks_response = get_playlist_items(playlist_id)
tracks.status_code
tracks = tracks_response.json().get("items")
tracks[0].keys()

tracks = [i.get("track") for i in tracks if i.get("track")]
tracks[0].get("external_ids")
tracks[0]

track_ids = [i.get("id") for i in tracks if i.get("id")]
track_ids[0]
len(track_ids)

track_ids = ",".join(track_ids) if isinstance(track_ids, list) else track_ids


# def get_audio_features(track_ids: str | list[str]) -> requests.Response:
#     if isinstance(track_ids, list):
#         if len(track_ids) > 50:
#             raise ValueError(
#                 f"track_ids too long: {len(track_ids)}. Can only supply up to 50 ids."
#             )
#         track_ids = ",".join(track_ids)

#     url = "https://api.spotify.com/v1/audio-features"
#     response = requests.get(
#         url,
#         params={
#             "ids": track_ids,
#         },
#         headers={
#             "Authorization": f"Bearer {token}",
#         },
#     )
#     return response


track_features = get_audio_features(track_ids + track_ids)
track_features.ok
len(track_features.json().get("audio_features"))


# def get_artists(artist_ids: str | list[str]) -> requests.Response:
#     if isinstance(artist_ids, list):
#         if len(artist_ids) > 50:
#             raise ValueError(
#                 f"track_ids too long: {len(artist_ids)}. Can only supply up to 50 ids."
#             )
#         artist_ids = ",".join(artist_ids)
#     url = "https://api.spotify.com/v1/artists"
#     response = requests.get(
#         url,
#         params={"ids": artist_ids},
#         headers={
#             "Authorization": f"Bearer {token}",
#         },
#     )
#     return response


tracks[0].keys()
# artists = [i.get('artists') for i in tracks if i.get('artists')]
artist_lists = [i.get("artists") for i in tracks if i.get("artists")]
artist_ids = []
for artist_list in artist_lists:
    artists_ids = [i.get("id") for i in artist_list if i.get("id")]
    artist_ids += artists_ids

len(artist_ids)

artists = get_artists(artist_ids[0])
artists.json()["artists"][0].keys()


def get_albums(album_ids: str | list[str]) -> requests.Response:
    if isinstance(album_ids, list):
        if len(album_ids) > 20:
            raise ValueError(
                f"track_ids too long: {len(album_ids)}. Can only supply up to 50 ids."
            )
        album_ids = ",".join(album_ids)
    url = "https://api.spotify.com/v1/albums"
    response = requests.get(
        url,
        params={"ids": album_ids},
        headers={
            "Authorization": f"Bearer {token}",
        },
    )
    return response


accessed_albums = [i.get("album") for i in tracks if i.get("album")]
album_ids = [i.get("id") for i in accessed_albums if i.get("id")]
len(album_ids)

res = get_albums(album_ids[0])


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
    def search_playlists(self, q: str, limit: int = 50, timeout=5) -> requests.Response:
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

    def get_playlist(self, playlist_id: str, timeout=5) -> requests.Response:
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
        response = requests.get(
            url,
            params={"fields": "tracks"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
        return response

    def get_playlist_items(
        self, playlist_id: str, limit: int = 50, offset: int = 0, timeout=5
    ) -> requests.Response:
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        response = requests.get(
            url,
            params={"limit": limit, "offset": offset},
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
        return response

    def get_audio_features(
        self, track_ids: str | list[str], timeout=5
    ) -> requests.Response:
        if isinstance(track_ids, list):
            if len(track_ids) > 50:
                raise ValueError(
                    f"track_ids too long: {len(track_ids)}. Can only supply up to 50 ids."
                )
            track_ids = ",".join(track_ids)

        url = "https://api.spotify.com/v1/audio-features"
        response = requests.get(
            url,
            params={
                "ids": track_ids,
            },
            headers={
                "Authorization": f"Bearer {token}",
            },
            timeout=timeout,
        )
        return response

    def get_artists(self, artist_ids: str | list[str], timeout=5) -> requests.Response:
        if isinstance(artist_ids, list):
            if len(artist_ids) > 50:
                raise ValueError(
                    f"track_ids too long: {len(artist_ids)}. Can only supply up to 50 ids."
                )
            artist_ids = ",".join(artist_ids)
        url = "https://api.spotify.com/v1/artists"
        response = requests.get(
            url,
            params={"ids": artist_ids},
            headers={
                "Authorization": f"Bearer {token}",
            },
            timeout=timeout,
        )
        return response

    def get_albums(self, album_ids: str | list[str], timeout=5) -> requests.Response:
        if isinstance(album_ids, list):
            if len(album_ids) > 20:
                raise ValueError(
                    f"track_ids too long: {len(album_ids)}. Can only supply up to 50 ids."
                )
            album_ids = ",".join(album_ids)
        url = "https://api.spotify.com/v1/albums"
        response = requests.get(
            url,
            params={"ids": album_ids},
            headers={
                "Authorization": f"Bearer {token}",
            },
            timeout=timeout,
        )
        return response

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
