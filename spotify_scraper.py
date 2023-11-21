from typing import Callable
import time
import logging
import json
import random
import base64
import requests


class SpotifyExtractor:
    def __init__(self, timeout=20) -> None:
        # load credentials
        self.clients = self.load_clients("credentials.json")
        self.picked_clients = set()
        self.pick_random_client()

        self.timeout = 20

        # authenticate
        response = self.client_credentials_auth()

        # get token
        if response.ok:
            self.token = response.json().get("access_token")
        else:
            raise requests.exceptions.HTTPError(response.text)

        self.logger = logging.getLogger(__name__)
        self.timeout = timeout
        self.failed_ids = {
            "playlists": list(),
            "track_features": list(),
            "artists": list(),
            "albums": list(),
        }

    def load_clients(self, json_file_path):
        with open(json_file_path, "r") as file:
            return json.load(file)

    def pick_random_client(self):
        available_clients = [
            client
            for client in self.clients
            if client["client_id"] not in self.picked_clients
        ]

        if not available_clients:
            print("All clients have been picked.")
            return None

        selected_client = random.choice(available_clients)
        self.picked_clients.add(selected_client["client_id"])

        self.client_id = selected_client.get("client_id")
        self.client_secret = selected_client.get("client_secret")

    def client_credentials_auth(self) -> requests.Response:
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
            timeout=self.timeout,
        )

        return response

    # actual endpoints we need to hit
    def search_playlists(self, q: str, limit: int = 50) -> requests.Response:
        url = "https://api.spotify.com/v1/search"
        params = {
            "q": q,
            "type": "playlist",
            "limit": limit,
        }
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(
            url=url, params=params, headers=headers, timeout=self.timeout
        )
        return response

    def get_playlist(self, playlist_id: str) -> requests.Response:
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
        response = requests.get(
            url,
            params={"fields": "tracks"},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=self.timeout,
        )
        return response

    def get_playlist_items(
        self, playlist_id: str, limit: int = 50, offset: int = 0
    ) -> requests.Response:
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        response = requests.get(
            url,
            params={"limit": limit, "offset": offset},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=self.timeout,
        )
        return response

    def get_audio_features(self, track_ids: str | list[str]) -> requests.Response:
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
            timeout=self.timeout,
        )
        return response

    def get_artists(self, artist_ids: str | list[str]) -> requests.Response:
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
            timeout=self.timeout,
        )
        return response

    def get_albums(self, album_ids: str | list[str]) -> requests.Response:
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
            timeout=self.timeout,
        )
        return response

    def handle_requests(self, requester: Callable, *args, **kwargs) -> dict:
        try:
            response = requester(*args, **kwargs)
            while not response.ok:
                if response.status_code == 429:
                    print("rate limit")
                    wait_time = response.headers.get("retry-after")
                    if (
                        len(self.picked_clients) < len(self.clients)
                    ) and wait_time > 500:
                        print("getting new client")
                        self.pick_random_client()
                        self.client_credentials_auth()
                    else:
                        # wait
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
                    print("404, continuing")
                    return None
                else:
                    raise requests.exceptions.HTTPError(response.text)
                response = requester(*args, **kwargs)

            return response.json()
        except requests.exceptions.ConnectionError:
            print("Connection reset or something, getting new client, waiting")
            time.sleep(10)
            self.pick_random_client()
            self.client_credentials_auth()
            return None

    # methods that handle iterating through the various endpoints
    def get_all_playlists(self, genre) -> list[dict]:
        search_results = self.handle_requests(self.search_playlists, genre, limit=50)
        playlist_results = search_results.get("playlists")
        if playlist_results:
            playlist_items = playlist_results.get("items")
            playlists = [{**i, "genre": genre} for i in playlist_items]

            return playlists

    def get_all_songs_from_playlist(
        self, playlist_id: str, total: int, limit: int = 50, delay: int | float = 0.5
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
            )
            if track_results:
                playlist_items += track_results.get("items")
            offset += limit
            time.sleep(delay)

        playlist_tracks = [i.get("track") for i in playlist_items if i.get("track")]

        return [{**i, "playlist_id": playlist_id} for i in playlist_tracks]

    def get_all_songs_from_playlists(
        self, playlists: list[dict], limit: int = 50, delay: int | float = 0.5
    ) -> list[dict]:
        tracks = []
        try:
            for i, playlist in enumerate(playlists):
                self.logger.info("Playlist: %s", i)
                print("Playlist: %s" % i)
                print(playlist.get("name"))
                playlist_id = playlist.get("id")
                number_tracks = playlist.get("tracks").get("total")
                tracks += self.get_all_songs_from_playlist(
                    playlist_id, number_tracks, limit=limit
                )
                time.sleep(delay)
        except Exception as e:
            print(f"Exception occurred, saving progress, {str(e)}")
            self.failed_ids["playlists"] += playlist_id
            return tracks

        return tracks

    def get_all_track_features(
        self, track_ids, limit: int = 100, delay: int | float = 0.5
    ) -> list[dict]:
        offset = 0
        total = len(track_ids)
        track_features = []
        self.logger.info("Total Songs: %s", total)
        print("Total Songs: %s" % total)
        try:
            while offset < total:
                self.logger.info("Offset: %s", offset)
                print("Offset: %s" % offset)
                id_chunk = track_ids[offset : offset + limit]
                features = self.handle_requests(self.get_audio_features, id_chunk)
                if features:
                    track_features += features.get("audio_features")
                offset += limit
                time.sleep(delay)
        except Exception as e:
            print(f"Exception occurred, saving progress, {str(e)}")
            self.failed_ids["track_features"] += track_ids[offset:]
            return track_features

        return track_features

    def get_all_artists(
        self, artist_ids: list[str], limit: int = 50, delay: int | float = 0.5
    ) -> list[dict]:
        offset = 0
        total = len(artist_ids)
        artists = []

        self.logger.info("Total Artists: %s", total)
        print("Total Artists: %s" % total)
        try:
            while offset < total:
                self.logger.info("Offset: %s", offset)
                print("Offset: %s" % offset)
                id_chunk = artist_ids[offset : offset + limit]
                if id_chunk:
                    artists_result = self.handle_requests(self.get_artists, id_chunk)
                    if artists_result:
                        artists += artists_result.get("artists")
                offset += limit
                print(artists[-1].get("name"))
                time.sleep(delay)
        except Exception as e:
            print(f"Exception occurred, saving progress, {str(e)}")
            self.failed_ids["artists"] += id_chunk
            return artists

        return artists

    def get_all_albums(
        self, album_ids: list[str], limit: int = 20, delay: int | float = 0.5
    ) -> list[dict]:
        offset = 0
        total = len(album_ids)
        albums = []

        self.logger.info("Total Albums: %s", total)
        print("Total Albums: %s" % total)
        try:
            while offset < total:
                self.logger.info("Offset: %s", offset)
                print("Offset: %s" % offset)
                id_chunk = album_ids[offset : offset + limit]
                if id_chunk:
                    album_chunk = self.handle_requests(self.get_albums, id_chunk)
                    if album_chunk:
                        albums += album_chunk["albums"]
                offset += limit
                print(albums[-1].get("name"))
                time.sleep(delay)
        except Exception as e:
            print(f"Exception occurred, saving progress, {str(e)}")
            self.failed_ids["albums"] += id_chunk
            return albums

        return albums
