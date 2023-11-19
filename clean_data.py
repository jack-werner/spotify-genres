import pandas as pd
import json
import os


def read_files(parent_directory: str, file_name: str):
    content = []
    entries = os.listdir(parent_directory)
    directories = [
        entry
        for entry in entries
        if os.path.isdir(os.path.join(parent_directory, entry))
    ]
    for directory in directories:
        path = f"{parent_directory}/{directory}/{file_name}.json"
        with open(path) as file:
            content += json.load(file)
    return content


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

# cast and save track-artists
tracks_artists.astype({"track_id": str, "album_id": str})
tracks_artists.to_parquet("track_artist.parquet", index=False)

# get tracks_playlists relationship
df_tracks[["id", "playlist_id"]].head()
df_tracks_playlists = df_tracks[["id", "playlist_id"]].drop_duplicates().dropna()
df_tracks_playlists.columns = ["track_id", "playlist_id"]


# save tracks_playlists
df_tracks_playlists.astype({"track_id": str, "playlist_id": str})
df_tracks_playlists.to_parquet("track_playlist", index=False)

# drop playlist id now that we have the edge table? and drop duplicates for tracks
df_tracks = df_tracks.drop(columns=["playlist_id"]).drop_duplicates()


# process track features
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

# TODO - save tracks with features
df_tracks_and_features = df_tracks_and_features.astype(
    {
        "id": str,
        "acousticness": float,
        "danceability": float,
        "energy": float,
        "instrumentalness": float,
        "speechiness": float,
        "key": int,
        "liveness": float,
        "loudness": float,
        "mode": int,
        "tempo": float,
        "time_signature": int,
        "valence": float,
        "duration_ms": int,
    }
)
df_tracks_and_features.to_parquet("tracks.parquet", index=False)


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
df_artists = df_artists.astype(
    {
        "id": str,
        "name": str,
        "popularity": int,
        "followers": int,
    }
)
df_artists.to_parquet("artists.parquet", index=False)


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
df_albums.astype(
    {
        "id": str,
        "name": str,
        "type": str,
        "release_date": "datetime64",
        "label": str,
    }
)


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

# TODO save playlist_genre table
df_playlist_genre.astype({"playlist_id": str, "genre_id": str})
df_playlist_genre.to_parquet("playlist_genre.parquet", index=False)


# drop genre from playlist and drop duplicates
df_playlists = df_playlists.drop(columns=["genre"]).drop_duplicates()

# TODO - save playlists
df_playlists.astype(
    {
        "id": str,
        "name": str,
        "description": str,
    }
)
df_playlists.to_parquet("playlists.parquet", index=False)
