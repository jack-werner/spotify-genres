import pandas as pd
import numpy
import json
import requests

# process tracks
with open("outputs/tracks.json", "r") as file:
    tracks = json.load(file)

df_tracks = pd.json_normalize(tracks)
df_tracks.columns

track_columns = [
    "id",
    "name",
    "popularity",
    "playlist_id",
    "album.id",
    "external_ids.isrc",
]


df_tracks = df_tracks[track_columns]
df_tracks = df_tracks.rename(
    columns={"album.id": "album_id", "external_ids.isrc": "isrc"}
)

# get track_artists relationship
df_tracks_exploded = df_tracks[["artists", "id"]].explode(column="artists")
artist_ids = (
    df_tracks_exploded["artists"].apply(lambda x: x.get("id")).reset_index(drop=True)
)
df_tracks_exploded["artists"] = artist_ids
df_tracks_exploded.columns = ["artist_id", "track_id"]
tracks_artists = df_tracks_exploded.drop_duplicates().dropna()

# TODO -- save track- artists

# get tracks_playlists relationship
df_tracks[["id", "playlist_id"]].head()
df_tracks_playlists = df_tracks[["id", "playlist_id"]].drop_duplicates().dropna()
df_tracks_playlists.columns = ["track_id", "playlist_id"]


# TODO -- save tracks_playlists

# drop playlist id now that we have the edge table? and drop duplicates for tracks
df_tracks = df_tracks.drop(columns=["playlist_id"]).drop_duplicates()


# process track analysises
with open("old_outputs/track_features.json", "r") as file:
    tracks_features = json.load(file)

df_features = pd.DataFrame(tracks_features)
feature_columns = [
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

df_features = df_features[feature_columns]

# join tracks and analysis together

df_tracks_and_features = df_tracks.merge(df_features, on="id").drop_duplicates()

# TODO - save tracks with analysis


# get artist data
with open("outputs/artists.json", "r") as file:
    artists = json.load(file)

df_artists = pd.json_normalize(artists)

artist_columns = [
    "id",
    "name",
    "popularity",
    "followers.total",
]

df_artists = df_artists[artist_columns]
df_artists = df_artists.rename(
    columns={"followers.total": "followers"}
).drop_duplicates()

# TODO -- cast datatypes and save artists


# get album data

with open("outputs/albums.json", "r") as file:
    albums = json.load(file)

df_albums = pd.json_normalize(albums)

# explode artists to get album-artists relationship for collabs and compilations

df_albums_exploded = df_albums[["artists", "id"]].explode(column="artists")
artist_ids = (
    df_albums_exploded["artists"].apply(lambda x: x.get("id")).reset_index(drop=True)
)
df_albums_exploded["artists"] = artist_ids
df_albums_exploded.columns = ["artist_id", "album_id"]
album_artists = df_albums_exploded.drop_duplicates().dropna()

album_columns = [
    "id",
    "name",
    "type",
    "release_date",
    "release_date_precision",
    "label",
]

df_albums = df_albums[album_columns]
df_albums = df_albums.drop_duplicates()


def handle_dates(row):
    if row["release_date_precision"] == "year":
        return f"{row['release_date']}-01-01"
    elif row["release_date_precision"] == "month":
        return f"{row['release_date']}-01"
    else:
        return row["release_date"]


df_albums["release_date"] = df_albums.apply(handle_dates, axis=1)
df_albums = df_albums.drop(columns=["release_date_precision"])

# TODO - cast dates to dates and other columns to better datatypes and save albums


# handle playlists
with open("new_outputs/playlists.json", "r") as file:
    playlists = json.load(file)

df_playlists = pd.json_normalize(playlists)

playlist_columns = ["id", "name", "description", "genre"]

df_playlists = df_playlists[playlist_columns]

# join genre to playlist
df_genre = pd.read_csv("genres.csv")
df_playlists_and_genres = df_playlists.merge(
    df_genre, left_on="genre", right_on="name", suffixes=("_playlist", "_genre")
)
df_playlist_genre = (
    df_playlists_and_genres[["id_playlist", "id_genre"]].drop_duplicates().dropna()
)
df_playlist_genre.columns = ["playlist_id", "genre_id"]

# TODO dave playlist_genre table


# drop genre from playlist and drop duplicates
df_playlists = df_playlists.drop(columns=["genre"]).drop_duplicates()

# TODO - save playlists
