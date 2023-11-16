# import json
# import os
# import sys
# import traceback
# from functools import reduce

# import numpy as np
# import pandas as pd
# import requests
# import spotipy
# from spotipy.oauth2 import SpotifyOAuth


# from dotenv import load_dotenv
# import os

# load_dotenv()


# import spotipy
# from spotipy.oauth2 import SpotifyOAuth


# sp = spotipy.Spotify(
#     auth_manager=SpotifyOAuth(
#         client_id=os.environ.get("CLIENT_ID"),
#         client_secret=os.environ.get("CLIENT_SECRET"),
#         redirect_uri=os.environ.get("REDIRECT_URI"),
#         scope="user-library-read",
#     )
# )

# results = sp.current_user_saved_tracks()
# for idx, item in enumerate(results["items"]):
#     track = item["track"]
#     print(idx, track["artists"][0]["name"], " – ", track["name"])


import spotipy
import sys
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time
import json

load_dotenv()

# Try with client credentials, this will be more easily implemented on AWS

# get genres
spotify = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=os.environ.get("CLIENT_ID"),
        client_secret=os.environ.get("CLIENT_SECRET"),
    )
)
genres = spotify.recommendation_genre_seeds().get("genres")
for g in genres:
    print(g)

# get playlists
playlists = []
for genre in genres:
    playlist_results = spotify.search(q=genre, limit=50, type="playlist")
    if playlist_results:
        playlists += playlist_results.get("playlists").get("items")


len(playlists)
# playlist_results = spotify.search(q=genres[0], limit=50, type="playlist")

# type(playlist_results.get('playlists').get('items')[0].keys())
# len(playlist_results.get('playlists').get('items'))


# playlist_results.get('playlists').get('items')[0].keys()
# playlist_results.get('playlists').get('items')[0]

# get track links for all playlists
playlists[0].keys()
playlists[0].get("id")
playlists[0].get("tracks").get("total")

playlist_ids = [p.get("id") for p in playlists]

# get tracks for all the playlists
tracks = []
for playlist_id in playlist_ids:
    pass


# going to have to iterate through the track items so let's take a look at what that looks like -- this should be its own function
playlist_id = playlists[0].get("id")
total_tracks = playlists[0].get("tracks").get("total")
playlist_tracks = []
offset = 0
limit = 100
# going to store what I think the length of playlist tracks should be just so that
# incase spotify has trouble retrieving a track, we don't infinite loop
while offset < total_tracks:
    print(offset)
    track_results = spotify.playlist_items(
        playlist_id=playlist_ids[0],
        additional_types=["track"],
        limit=limit,
        offset=offset,
    )
    if track_results:
        # TODO something to handle rate limiting in here
        playlist_tracks += track_results.get("items")
    offset += limit

len(playlist_tracks)
playlist_tracks[0]


def get_playlists_tracks(playlist_id, total, limit=100):
    playlist_tracks = []
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
            playlist_tracks += track_results.get("items")
        offset += limit

    return playlist_tracks


playlist_tracks = get_playlists_tracks(
    playlist_id=playlist_ids[0],
    total=playlists[0].get("tracks").get("total"),
    limit=100,
)

# ok now get all the tracks.

start = time.perf_counter()
tracks = []
for i, playlist in enumerate(playlists[:100]):
    print(i)
    playlist_id = playlist.get("id")
    number_tracks = playlist.get("tracks").get("total")
    tracks += get_playlists_tracks(playlist_id, number_tracks, 100)

end = time.perf_counter()
print("duration", end - start)
print("tracks", len(tracks))

# took, about 1.5 minutes for us to go through 100 playlists, yielding us 13k tracks

# for i in playlist_tracks[:20]:
#     print(type(i))

# playlist_tracks[2]

# playlist_tracks = spotify.playlist_items(playlist_id=playlist_ids[0], additional_types=['track'], limit=100)

# type(playlist_tracks)
# playlist_tracks.keys()
# playlist_tracks.get('next')
# playlist_tracks.get('offset')
# playlist_tracks.get('total')


# get track features for all tracks

tracks[0].keys()
tracks[0].get("track").keys()
tracks[0].get("track").get("id")


len(track_ids)

spotify.audio_features(tracks[0].get("track").get("id"))

track_features = spotify.audio_features(track_ids)
track_features[0].keys()

# for track in tracks:
#     pass

# some of these don't actually have tracks - if makes sure that we will have actual tracks
accessed_tracks = [i.get("track") for i in tracks if i.get("track")]
all(accessed_tracks)

track_ids = [i.get("id") for i in accessed_tracks if i.get("id")]
len(track_ids)


# track_ids = [i.get("track").get("id") for i in tracks]

# offset = 0
# limit = 100
# total = len(track_ids)
# track_features = []

# start = time.perf_counter()
# while offset < total:
#     print(offset)
#     # TODO maybe refactor to be simpler, this check might not actually be  necessary
#     id_chunk = track_ids[offset : offset + limit]
#     if id_chunk:
#         features = spotify.audio_features(track_ids[offset : offset + limit])
#         track_features += features
#     offset += limit
# end = time.perf_counter()

print("duration", end - start)
print("features", len(track_features))


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
track_features = get_all_track_features(track_ids, 100)
end = time.perf_counter()
print("duration", end - start)

# takes about 35 seconds to get 13k track features

# get artist details
accessed_tracks[0].keys()
accessed_tracks[0].get("artists")[0].get("id")

accessed_artists = [
    i.get("artists") for i in accessed_tracks if i.get("artists")
]  # all of these will be lists, some of which with more than one item
artist_ids = []
for artists in accessed_artists:
    artists_ids = [i.get("id") for i in artists if i.get("id")]
    artist_ids += artists_ids

len(accessed_artists)
len(artist_ids)

# maybe want to filter down the number of artist Ids to just uniques. probably getting a lot of artists multiple times
# with multiple artists on the same tracks, especially with collabs.

artist_ids[0]


artists = spotify.artists(artist_ids[:10])
artists.get("artists")[0].keys()
artists.keys()

artist_ids = list(set(artist_ids))

len(artist_ids)


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

# took 2 minutes to get 18k artists
# took 30 seconds to get 5k artists


# get album details
accessed_tracks[0].get("album").get("id")

accessed_albums = [i.get("album") for i in accessed_tracks if i.get("album")]
album_ids = [i.get("id") for i in accessed_albums if i.get("id")]
len(album_ids)
len(set(album_ids))

album_ids = list(set(album_ids))

albums = spotify.albums(album_ids[:10])

albums.keys()


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

# takes 193 seconds (3.25 min) to get 8k albums

len(set(album_ids))
len(set(track_ids))


# write to JSON

filepaths = {
    "track_features": track_features,
    "tracks": tracks,
    "artists": artists,
    "albums": albums,
    "playlists": playlists,
}

# write genres -- will this be a table? in s3? or do we just partition by genre
# write track analysis
output_path = "outputs"
with open(f"{output_path}/track_features.json", "w", encoding="utf-8") as file:
    json.dump(track_features, file)

# write tracks
with open(f"{output_path}/tracks.json", "w", encoding="utf-8") as file:
    json.dump(tracks, file)

# write artists
len(all_artists)
with open(f"{output_path}/artists.json", "w", encoding="utf-8") as file:
    json.dump(all_artists, file)

# write albums -- need
len(all_albums)
with open(f"{output_path}/albums.json", "w", encoding="utf-8") as file:
    json.dump(all_albums, file)

# write playlists
len(playlists)
with open(f"{output_path}/playlists.json", "w", encoding="utf-8") as file:
    json.dump(playlists, file)


# back of the napkin math
# total execution time =
#   genres
#   * (
#       + (90 seconds for 50*200 tracks)
#       + (30 seconds for 10k track features)
#       + (30 seconds for 5k artists )
#       + (198 seconds for 8k albums )
#   )

"""
assuming we get about 10k tracks from the 50 playlists we pull for the genre,
and it takes 90 seconds to get those tracks, plus another 30 seconds for the features,
plus another 30 seconds to get all the artists and then another 3 minutes for all the albums

so it should take about 6 minutes to get all the data for about 10k songs.

I think we should be able to put each genre into a lambda to scale this out. can't have them all running at the same time though because then we will probably get rate limited


"""
