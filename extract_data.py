import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time
import json
import requests

load_dotenv()

output_path = "sub_outputs"

# get genres
auth = SpotifyClientCredentials(
    client_id=os.environ.get("CLIENT_ID"),
    client_secret=os.environ.get("CLIENT_SECRET"),
)
spotify = spotipy.Spotify(auth_manager=auth)
token = auth.get_access_token(as_dict=False)

genres = spotify.recommendation_genre_seeds().get("genres")
all_genres = genres

#### TEMP #####
### get genres from file
file_path = "limited-genres.txt"
with open(file_path, "r") as file:
    content_list = file.readlines()

genres = [line.strip() for line in content_list]

# len(genres)
# genres[1]


# get playlists
print("getting playlists")
playlists = []
for genre in genres:
    print(genre)
    search_results = spotify.search(q=genre, limit=50, type="playlist")
    if search_results:
        playlist_items = search_results.get("playlists").get("items")
        playlists += [{**i, "genre": genre} for i in playlist_items]


# print(playlists[0].keys())

playlist_ids = [p.get("id") for p in playlists]

# write playlists
len(playlists)
with open(f"{output_path}/playlists.json", "w", encoding="utf-8") as file:
    json.dump(playlists, file)


# get tracks from playlists
print("getting tracks from playlists")


def get_playlist_items(playlist_id, token):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    response = requests.get(
        url,
        params={"additional-types": "tracks"},
        headers={"Authorization": f"Bearer {token}"},
    )

    return response


def handle_playlist_items(playlist_id, token):
    response = get_playlist_items(playlist_id, token)

    while not response.ok:
        if response.status_code == 429:
            print("rate limit")
            wait_time = response.headers.get("retry-after")
            print("waiting", wait_time)
            time.sleep(1 + int(wait_time))

        elif response.status_code == 401:
            print("401")
            auth = SpotifyClientCredentials(
                client_id=os.environ.get("CLIENT_ID"),
                client_secret=os.environ.get("CLIENT_SECRET"),
            )
            # spotify = spotipy.Spotify(auth_manager=auth)
            token = auth.get_access_token(as_dict=False)
        elif response.status_code == 404:
            return None
        else:
            raise requests.exceptions.HTTPError(response.text)
        response = get_playlist_items(playlist_id, token)

    if response.ok:
        return response.json()


def get_playlists_tracks_api(token, playlist_id, total, limit=100):
    playlist_items = []
    offset = 0
    while offset < total:
        print(offset)
        track_results = handle_playlist_items(playlist_id, token)
        if track_results:
            # TODO something to handle rate limiting in here
            playlist_items += track_results.get("items")
        offset += limit

    # don't need data around when the tracks were added to the playlist
    playlist_tracks = [i.get("track") for i in playlist_items if i.get("track")]

    return [{**i, "playlist_id": playlist_id} for i in playlist_tracks]


def get_playlists_tracks(playlist_id, total, limit=100):
    playlist_items = []
    offset = 0
    while offset < total:
        print(offset)
        track_results = spotify.playlist_items(
            playlist_id=playlist_id,
            additional_types=["track"],
            limit=limit,
            offset=offset,
        )
        if track_results:
            # TODO something to handle rate limiting in here
            playlist_items += track_results.get("items")
        offset += limit

    # don't need data around when the tracks were added to the playlist
    playlist_tracks = [i.get("track") for i in playlist_items if i.get("track")]

    return [{**i, "playlist_id": playlist_id} for i in playlist_tracks]


start = time.perf_counter()
tracks = []
for i, playlist in enumerate(playlists):
    print("playlist", i)

    playlist_id = playlist.get("id")
    number_tracks = playlist.get("tracks").get("total")
    print("total_tracks", number_tracks)
    # tracks += get_playlists_tracks(playlist_id, number_tracks, 100)
    tracks += get_playlists_tracks_api(token, playlist_id, number_tracks, 100)
    time.sleep(1)  # to avoid running into rate limit issues

end = time.perf_counter()
print("duration", end - start)
print("tracks", len(tracks))

### TEMP ####
## continuing to get tracks from the playlists
# continued_tracks = tracks.copy()

start = time.perf_counter()
for i, playlist in enumerate(playlists[1367 + 597 + 650 :]):
    print("playlist", i)

    playlist_id = playlist.get("id")
    number_tracks = playlist.get("tracks").get("total")
    print("total_tracks", number_tracks)
    # tracks += get_playlists_tracks(playlist_id, number_tracks, 100)
    continued_tracks += get_playlists_tracks_api(token, playlist_id, number_tracks, 100)
    print("accumulated_tracks", len(continued_tracks))
    time.sleep(1)  # to avoid running into rate limit issues

end = time.perf_counter()
print("duration", end - start)
# print("tracks", len(tracks))
print("tracks", len(continued_tracks))

tracks = continued_tracks.copy()


# save tracks
with open(f"{output_path}/tracks.json", "w", encoding="utf-8") as file:
    json.dump(tracks, file)

with open(f"checkpoint/tracks.json", "w", encoding="utf-8") as file:
    json.dump(continued_tracks, file)


track_ids = [i.get("id") for i in tracks if i.get("id")]
len(track_ids)


def get_all_track_features(track_ids, limit):
    if limit > 100:
        raise ValueError("limit must be less than 100")

    offset = 0
    total = len(track_ids)
    track_features = []

    while offset < total:
        print(offset)
        # TODO maybe refactor to be simpler, this check might not actually be  necessary
        id_chunk = track_ids[offset : offset + limit]
        if id_chunk:
            features = spotify.audio_features(id_chunk)
            track_features += features
        offset += limit
        if offset % 1000 == 0:
            time.sleep(1)

    return track_features


start = time.perf_counter()
all_track_features = get_all_track_features(track_ids, 100)
end = time.perf_counter()
print("duration", end - start)


# save
with open(f"{output_path}/track_features.json", "w", encoding="utf-8") as file:
    json.dump(all_track_features, file)


# get all artists

artist_lists = [i.get("artists") for i in tracks if i.get("artists")]
artist_ids = []
for artist_list in artist_lists:
    artists_ids = [i.get("id") for i in artist_list if i.get("id")]
    artist_ids += artists_ids

# remove duplicate artists
artist_ids = list(set(artist_ids))


def get_all_artists(artist_ids, limit: int):
    if limit > 50:
        raise ValueError("limit must be less than 50")

    offset = 0
    total = len(artist_ids)
    artists = []

    while offset < total:
        print(offset)
        id_chunk = artist_ids[offset : offset + limit]
        if id_chunk:
            artist_chunk = spotify.artists(id_chunk)
            # probably actually need to decompose this a little more
            artists += artist_chunk.get("artists")
        offset += limit

    return artists


start = time.perf_counter()
all_artists = get_all_artists(artist_ids, 50)
end = time.perf_counter()
print("duration", end - start)

# save artists
len(all_artists)
with open(f"{output_path}/artists.json", "w", encoding="utf-8") as file:
    json.dump(all_artists, file)

# get all albums
accessed_albums = [i.get("album") for i in tracks if i.get("album")]
album_ids = [i.get("id") for i in accessed_albums if i.get("id")]
album_ids = list(set(album_ids))


def get_all_albums(album_ids, limit: int):
    if limit > 20:
        raise ValueError("limit must be less than 20")

    offset = 0
    total = len(album_ids)
    albums = []

    while offset < total:
        print(offset)
        id_chunk = album_ids[offset : offset + limit]
        if id_chunk:
            album_chunk = spotify.albums(id_chunk)
            albums += album_chunk.get("albums")
        offset += limit

    return albums


start = time.perf_counter()
all_albums = get_all_albums(album_ids, 20)
end = time.perf_counter()
print("duration", end - start)


# write to JSON
# with open(f"{output_path}/track_features.json", "w", encoding="utf-8") as file:
#     json.dump(all_track_features, file)

# write tracks
# with open(f"{output_path}/tracks.json", "w", encoding="utf-8") as file:
#     json.dump(tracks, file)

# write artists
# len(all_artists)
# with open(f"{output_path}/artists.json", "w", encoding="utf-8") as file:
#     json.dump(all_artists, file)

# write albums
# len(all_albums)
# with open(f"{output_path}/albums.json", "w", encoding="utf-8") as file:
#     json.dump(all_albums, file)

# write playlists
# len(playlists)
# with open(f"{output_path}/playlists.json", "w", encoding="utf-8") as file:
#     json.dump(playlists, file)
