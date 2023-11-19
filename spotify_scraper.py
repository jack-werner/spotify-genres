import pandas as pd
import requests
import dotenv
import base64
import os
import time
import logging
from typing import Callable

# dotenv.load_dotenv()


# def client_credentials_auth(client_id, client_secret, timeout=5) -> requests.Response:
#     url = "https://accounts.spotify.com/api/token"
#     auth_string = f"{client_id}:{client_secret}"
#     encoded = base64.b64encode(
#         (client_id + ":" + client_secret).encode("ascii")
#     ).decode("ascii")

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


# client_id = os.environ.get("CLIENT_ID")
# client_secret = os.environ.get("CLIENT_SECRET")

# auth = client_credentials_auth(client_id, client_secret)
# auth.status_code
# token = auth.json().get("access_token")
# token


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

# playlist_id = "2brN3U7zhnFHhoKROkzfA9"

# playlist = get_playlist(playlist_id)
# playlist.status_code
# playlist.json().get("tracks").get("total")

# tracks = playlist.json().get("tracks")
# tracks.keys()
# tracks.get("next")

# tracks_response = tracks
# tracks_response = get_playlist_items(playlist_id)
# tracks.status_code
# tracks = tracks_response.json().get("items")
# tracks[0].keys()

# tracks = [i.get("track") for i in tracks if i.get("track")]
# tracks[0].get("external_ids")
# tracks[0]

# track_ids = [i.get("id") for i in tracks if i.get("id")]
# track_ids[0]
# len(track_ids)

# track_ids = ",".join(track_ids) if isinstance(track_ids, list) else track_ids


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


# track_features = get_audio_features(track_ids + track_ids)
# track_features.ok
# len(track_features.json().get("audio_features"))


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


# tracks[0].keys()
# # artists = [i.get('artists') for i in tracks if i.get('artists')]
# artist_lists = [i.get("artists") for i in tracks if i.get("artists")]
# artist_ids = []
# for artist_list in artist_lists:
#     artists_ids = [i.get("id") for i in artist_list if i.get("id")]
#     artist_ids += artists_ids

# len(artist_ids)

# artists = get_artists(artist_ids[0])
# artists.json()["artists"][0].keys()


# def get_albums(album_ids: str | list[str]) -> requests.Response:
#     if isinstance(album_ids, list):
#         if len(album_ids) > 20:
#             raise ValueError(
#                 f"track_ids too long: {len(album_ids)}. Can only supply up to 50 ids."
#             )
#         album_ids = ",".join(album_ids)
#     url = "https://api.spotify.com/v1/albums"
#     response = requests.get(
#         url,
#         params={"ids": album_ids},
#         headers={
#             "Authorization": f"Bearer {token}",
#         },
#     )
#     return response


# accessed_albums = [i.get("album") for i in tracks if i.get("album")]
# album_ids = [i.get("id") for i in accessed_albums if i.get("id")]
# len(album_ids)

# res = get_albums(album_ids[0])


# def handle_requests(requester: Callable, *args, **kwargs) -> requests.Response:
#     response = requester(*args, **kwargs)
#     while not response.ok:
#         if response.status_code == 429:
#             print("rate limit")
#             wait_time = response.headers.get("retry-after")
#             print("waiting", wait_time)
#             time.sleep(1 + int(wait_time))
#         elif response.status_code == 401:
#             print("401 Error, refreshing token")
#             auth = client_credentials_auth(client_id, client_secret)
#             if auth.ok:
#                 token = auth.json().get("access_token")
#             else:
#                 raise requests.exceptions.HTTPError(auth.text)
#         elif response.status_code == 404:
#             return None
#         else:
#             raise requests.exceptions.HTTPError(response.text)
#         response = requester(*args, **kwargs)

#     return response


# res = handle_requests(get_albums, album_ids[:20])
# res = handle_requests(get_playlist, playlist_id)
# res.json().get('tracks')


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

        self.logger = logging.getLogger(__name__)

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
            headers={"Authorization": f"Bearer {self.token}"},
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
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=timeout,
        )
        return response

    def get_audio_features(
        self, track_ids: str | list[str], timeout=5
    ) -> requests.Response:
        if isinstance(track_ids, list):
            if len(track_ids) > 100:
                raise ValueError(
                    f"track_ids too long: {len(track_ids)}. Can only supply up to 100 ids."
                )
            track_ids = ",".join(track_ids)

        url = "https://api.spotify.com/v1/audio-features"
        response = requests.get(
            url,
            params={
                "ids": track_ids,
            },
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            timeout=timeout,
        )
        return response

    def get_artists(self, artist_ids: str | list[str], timeout=5) -> requests.Response:
        if isinstance(artist_ids, list):
            if len(artist_ids) > 50:
                raise ValueError(
                    f"artist_ids too long: {len(artist_ids)}. Can only supply up to 50 ids."
                )
            artist_ids = ",".join(artist_ids)
        url = "https://api.spotify.com/v1/artists"
        response = requests.get(
            url,
            params={"ids": artist_ids},
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            timeout=timeout,
        )
        return response

    def get_albums(self, album_ids: str | list[str], timeout=5) -> requests.Response:
        if isinstance(album_ids, list):
            if len(album_ids) > 20:
                raise ValueError(
                    f"album_ids too long: {len(album_ids)}. Can only supply up to 20 ids."
                )
            album_ids = ",".join(album_ids)
        url = "https://api.spotify.com/v1/albums"
        response = requests.get(
            url,
            params={"ids": album_ids},
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            timeout=timeout,
        )
        return response

    # helper method for handling rate limiting, token refresh, and 404 errors
    def handle_requests(self, requester: Callable, *args, **kwargs) -> dict:
        response = requester(*args, **kwargs)
        while not response.ok:
            if response.status_code == 429:
                print("rate limit")
                wait_time = response.headers.get("retry-after")
                print("waiting", wait_time)
                time.sleep(1 + int(wait_time))
            elif response.status_code == 401:
                print("401 Error, refreshing token")
                auth = self.client_credentials_auth()
                if auth.ok:
                    self.token = auth.json().get("access_token")
                else:
                    raise requests.exceptions.HTTPError(auth.text)
            elif response.status_code == 404:
                return None
            else:
                raise requests.exceptions.HTTPError(response.text)
            response = requester(*args, **kwargs)

        return response.json()

    # methods that handle iterating through the various endpoints
    def get_all_playlists(self, genre) -> list[dict]:
        search_results = self.handle_requests(self.search_playlists, genre, limit=50)
        playlist_items = search_results.get("playlists").get("items")
        playlists = [{**i, "genre": genre} for i in playlist_items]

        return playlists

    def get_all_songs_from_playlist(
        self, playlist_id: str, total: int, limit: int = 50, timeout=5
    ) -> list[dict]:
        playlist_items = []
        offset = 0
        self.logger.info("Total Songs: %s", total)
        print("Total Songs: %s" % total)
        while offset < total:
            self.logger.info("Offset: %s", offset)
            print("Offset: %s" % offset)
            track_results = self.handle_requests(
                self.get_playlist_items,
                playlist_id,
                limit=limit,
                offset=offset,
                timeout=timeout,
            )
            playlist_items += track_results.get("items")
            offset += limit

        playlist_tracks = [i.get("track") for i in playlist_items if i.get("track")]

        return [{**i, "playlist_id": playlist_id} for i in playlist_tracks]

    def get_all_songs_from_playlists(
        self, playlists: list[dict], limit: int = 50, delay: int | float = 0.5
    ) -> list[dict]:
        tracks = []
        for i, playlist in enumerate(playlists):
            self.logger.info("Playlist: %s", i)
            print("Playlist: %s" % i)
            playlist_id = playlist.get("id")
            number_tracks = playlist.get("tracks").get("total")
            tracks += self.get_all_songs_from_playlist(
                playlist_id, number_tracks, limit=limit
            )
            time.sleep(delay)
        return tracks

    def get_all_track_features(self, track_ids, limit: int = 100) -> list[dict]:
        offset = 0
        total = len(track_ids)
        track_features = []
        self.logger.info("Total Songs: %s", total)
        print("Total Songs: %s" % total)
        while offset < total:
            self.logger.info("Offset: %s", offset)
            print("Offset: %s" % offset)
            id_chunk = track_ids[offset : offset + limit]
            if id_chunk:
                features = self.handle_requests(self.get_audio_features, id_chunk)
                track_features += features
            offset += limit

        return track_features

    def get_all_artists(self, artist_ids: list[str], limit: int = 50) -> list[dict]:
        offset = 0
        total = len(artist_ids)
        artists = []

        self.logger.info("Total Artists: %s", total)
        print("Total Artists: %s" % total)
        while offset < total:
            self.logger.info("Offset: %s", offset)
            print("Offset: %s" % offset)
            id_chunk = artist_ids[offset : offset + limit]
            if id_chunk:
                artists_result = self.handle_requests(self.get_artists, id_chunk)
                artists += artists_result.get("artists")
            offset += limit

        return artists

    def get_all_albums(self, album_ids: list[str], limit: int = 20) -> list[dict]:
        offset = 0
        total = len(album_ids)
        albums = []

        self.logger.info("Total Albums: %s", total)
        print("Total Albums: %s" % total)
        while offset < total:
            self.logger.info("Offset: %s", offset)
            print("Offset: %s" % offset)
            id_chunk = album_ids[offset : offset + limit]
            if id_chunk:
                album_chunk = self.handle_requests(self.get_albums, id_chunk)
                albums += album_chunk["albums"]
            offset += limit

        return albums
