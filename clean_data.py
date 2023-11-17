import pandas as pd
import numpy
import json
import requests

# all the relationship things we need to take care of
track_artists = []
track_playlists = []
album_artists = []
# track_genres = []

# this is where we need to basically get everything into our data model I think? atleast we need to deduplicate stuff

# process tracks

with open("outputs/tracks.json", "r") as file:
    tracks = json.load(file)

df_tracks = pd.json_normalize(tracks)
df_tracks.columns

top_columns = [
    "id",
    "name",
    "popularity",
    "playlist_id",
    "album.id",
    "external_ids.isrc",
]


df_tracks_new = df_tracks[top_columns]
df_tracks_new.head()


# get track_artists relationship
df_exploded = df_tracks[["artists", "id"]].explode(column="artists")
artist_ids = df_exploded["artists"].apply(lambda x: x.get("id")).reset_index(drop=True)
df_exploded["artists"] = artist_ids
df_exploded.columns = ["artist_id", "track_id"]
tracks_artists = df_exploded
tracks_artists.head()

# TODO get tracks_playlists relationship


# sanity check that this relationship makes sense -=- TODO actually drop duplicates here
# len(tracks_artists.drop_duplicates())
# len(tracks_artists['track_id'].unique())

# track_artists = df_tracks['artists'][0]
# df_tracks['artists'].map(lambda x: [x.get('id')])
# df_tracks.shape

# json_norm = list(df_tracks[['artists', 'id']].apply(pd.json_normalize))
# len(json_norm)
# df_artists = pd.concat(json_norm)
# df_artists.shape
# df_artists.columns


# df_artists = pd.json_normalize(df_tracks[['artists', 'id']])


# process track analysises
with open("old_outputs/track_features.json", "r") as file:
    tracks_features = json.load(file)

df_features = pd.DataFrame(tracks_features)
df_features.columns
top_columns = [
    "id",
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "speechiness",
    "key",
    "liveness",
    "loudness",
    "mode",
    "tempo",
    "time_signature",
    "valence",
    "duration_ms",
]

df_features = df_features[top_columns]


# actually were just gonna add all the analysis features to the track table because
# its only relation is to the track and contains all the metrics. without doing this
# tracks don't have manyfacts and the analysis doesn't have much reason for existing

# join tracks and analysis together

df_tracks_and_features = df_tracks_new.merge(df_features, on="id").drop_duplicates()
df_tracks_and_features.head()


# get artist data
with open("outputs/artists.json", "r") as file:
    artists = json.load(file)

df_artists = pd.json_normalize(artists)

df_artists.columns

top_columns = [
    "id",
    "name",
    "popularity",
    "followers.total",
]

df_artists = df_artists[top_columns]
df_artists = df_artists.rename(columns={"followers.total": "followers"})
df_artists.head()


# get album data

with open("outputs/albums.json", "r") as file:
    albums = json.load(file)

df_albums = pd.json_normalize(albums)

df_albums.columns

album_columns = [
    "id",
    "name",
    "type",
    "release_date",
    "release_date_precision",
    "label",
]


df_albums = df_albums[album_columns]


# explode the artists thing like we did for tracks

df_albums_exploded = df_albums[["artists", "id"]].explode(column="artists")
artist_ids = (
    df_albums_exploded["artists"].apply(lambda x: x.get("id")).reset_index(drop=True)
)
df_albums_exploded["artists"] = artist_ids
df_albums_exploded.columns = ["artist_id", "album_id"]
album_artists = df_albums_exploded
album_artists.head()

# len(album_artists['artist_id'].unique())
# len(album_artists['album_id'].unique())

# album_artists.groupby('album_id').count().reset_index().sort_values('artist_id', ascending=False)


# to impute the dates for all of these so that it is easier to work with,
# were gonna write a little apply function

# df_albums[df_albums["release_date_precision"] == "year"].head()


def handle_dates(row):
    if row["release_date_precision"] == "year":
        return f"{row['release_date']}-01-01"
    elif row["release_date_precision"] == "month":
        return f"{row['release_date']}-01"
    else:
        return row["release_date"]


df_albums["release_date"] = df_albums.apply(handle_dates, axis=1)
df_albums = df_albums.drop(columns=["release_date_precision"])

# TODO - cast dates to dates and other columns to better datatypes


# handle playlists
load_dotenv()

with open("new_outputs/playlists.json", "r") as file:
    playlists = json.load(file)

df_playlists = pd.json_normalize(playlists)
df_playlists.columns

playlist_columns = ["id", "name", "description", "genre"]

df_playlists = df_playlists[playlist_columns]

playlist_ids = [p.get("id") for p in playlists]

# playlist_ids[0]

get_playlists_tracks(playlist_ids[0], 200, 100)


# TESTING
auth = SpotifyClientCredentials(
    client_id=os.environ.get("CLIENT_ID"),
    client_secret=os.environ.get("CLIENT_SECRET"),
)

spotify = spotipy.Spotify(auth_manager=auth)

token = auth.get_access_token(as_dict=False)
token
track_results = spotify.playlist_items(
    playlist_id=playlist_ids[0],
    additional_types=["track"],
    limit=100,
    offset=0,
)

response = requests.get(
    f"https://api.spotify.com/v1/playlists/{playlist_ids[0]}/tracks",
    # params={
    #     'q': q,
    #     'type': 'playlist',
    #     'limit': limit
    # },
    headers={"Authorization": f"Bearer {token}"},
)

import os

os.environ["CLIENT_SECRET"]

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


# request limit is by endpoint per account. different apps don't help
# spotify.


# join genre to track


# genre


# output_path = "new_outputs"
# with open(f"{output_path}/playlists.json", "w", encoding="utf-8") as file:
#     json.dump(playlists, file)
