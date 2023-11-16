import spotipy
import sys
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time
import json

load_dotenv()

# get genres
spotify = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=os.environ.get("CLIENT_ID"),
        client_secret=os.environ.get("CLIENT_SECRET"),
    )
)
genres = spotify.recommendation_genre_seeds().get("genres")

# get playlists
print("getting playlists")
playlists = []
for genre in genres:
    print(genre)
    search_results = spotify.search(q=genre, limit=50, type="playlist")
    if search_results:
        playlist_items = search_results.get("playlists").get("items")
        playlists += [{**i, "genre": genre} for i in playlist_items]

print(playlists[0].keys())

playlist_ids = [p.get("id") for p in playlists]


# get tracks from playlists
print("getting tracks from playlists")


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
    print(i)
    playlist_id = playlist.get("id")
    number_tracks = playlist.get("tracks").get("total")
    tracks += get_playlists_tracks(playlist_id, number_tracks, 100)

end = time.perf_counter()
print("duration", end - start)
print("tracks", len(tracks))


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

    return track_features


start = time.perf_counter()
all_track_features = get_all_track_features(track_ids, 100)
end = time.perf_counter()
print("duration", end - start)


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
output_path = "final_outputs"
with open(f"{output_path}/track_features.json", "w", encoding="utf-8") as file:
    json.dump(all_track_features, file)

# write tracks
with open(f"{output_path}/tracks.json", "w", encoding="utf-8") as file:
    json.dump(tracks, file)

# write artists
len(all_artists)
with open(f"{output_path}/artists.json", "w", encoding="utf-8") as file:
    json.dump(all_artists, file)

# write albums
len(all_albums)
with open(f"{output_path}/albums.json", "w", encoding="utf-8") as file:
    json.dump(all_albums, file)

# write playlists
len(playlists)
with open(f"{output_path}/playlists.json", "w", encoding="utf-8") as file:
    json.dump(playlists, file)
