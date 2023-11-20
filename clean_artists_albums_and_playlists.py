import pandas as pd
import json
import os
import file_reader

OUTPUT_PATH = "retry_outputs"

# get artist data
artists = file_reader.read_files(OUTPUT_PATH, "artists")

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

# cast datatypes and save artists
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
albums = file_reader.read_files(OUTPUT_PATH, "albums")
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


df_albums["release_date"] = df_albums.apply(file_reader.handle_dates, axis=1)
df_albums = df_albums.drop(columns=["release_date_precision"])

# cast dates to dates and other columns to better datatypes and save albums
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
playlists = file_reader.read_files(OUTPUT_PATH, "playlists")
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

# save playlist_genre table
df_playlist_genre.astype({"playlist_id": str, "genre_id": str})
df_playlist_genre.to_parquet("playlist_genre.parquet", index=False)

# drop genre from playlist and drop duplicates
df_playlists = df_playlists.drop(columns=["genre"]).drop_duplicates()

# save playlists
df_playlists.astype(
    {
        "id": str,
        "name": str,
        "description": str,
    }
)
df_playlists.to_parquet("playlists.parquet", index=False)