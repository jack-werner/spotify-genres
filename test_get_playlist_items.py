import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time
import json
import requests

load_dotenv()

os.environ["CLIENT_ID"]

with open("new_outputs/playlists.json", "r") as file:
    playlists = json.load(file)

playlist_ids = [p.get("id") for p in playlists]

# TESTING
auth = SpotifyClientCredentials(
    client_id=os.environ.get("CLIENT_ID"),
    client_secret=os.environ.get("CLIENT_SECRET"),
)

spotify = spotipy.Spotify(auth_manager=auth)

token = auth.get_access_token(as_dict=False)
token
# track_results = spotify.playlist_items(
#     playlist_id=playlist_ids[0],
#     additional_types=["track"],
#     limit=100,
#     offset=0,
# )

response = requests.get(
    f"https://api.spotify.com/v1/playlists/{playlist_ids[0]}/tracks",
    # params={
    #     'q': q,
    #     'type': 'playlist',
    #     'limit': limit
    # },
    headers={"Authorization": f"Bearer {token}"},
)


response.ok
response.headers["content-type"]
response.headers.get("content-type")
dict(response.headers).keys()

response.status_code

response.headers
response.headers["retry-after"]

response = requests.get(
    f"https://api.spotify.com/v1/audio-features/{tracks[0].get('id')}",
    params={"limit": limit},
    headers={"Authorization": f"Bearer {token}"},
)


def get_playlist_items(playlist_id, token):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    response = requests.get(
        url,
        params={"additional-types": "tracks"},
        headers={"Authorization": f"Bearer {token}"},
    )

    while response.status_code == 429:
        print("rate limit")
        wait_time = response.headers.get("retry-after")
        print("waiting", wait_time)
        time.sleep(1 + int(wait_time))

    if response.ok:
        return response.json()


playlist_stuff = get_playlist_items(playlist_ids[0], token)
playlist_stuff.get("items")[0].keys()

get_playlists_tracks(playlist_ids[0], 200, 100)
