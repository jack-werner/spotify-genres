import pandas as pd
import file_reader
import time

INPUT_PATH = "retry_outputs"
OUTPUT_PATH = "normalized"

# process tracks
print("reading tracks")
start = time.perf_counter()
tracks = file_reader.read_files(INPUT_PATH, "tracks")

df_tracks = pd.json_normalize(tracks)
track_columns = [
    "id",
    "name",
    "popularity",
    "playlist_id",
    "album.id",
    "external_ids.isrc",
    "artists",
]
df_tracks = df_tracks[track_columns]
df_tracks = df_tracks.rename(
    columns={"album.id": "album_id", "external_ids.isrc": "isrc"}
)

# get track_artists relationship
now = time.perf_counter()
print("getting track artists", now - start)
df_tracks_exploded = df_tracks[["artists", "id"]].explode(column="artists")
artist_ids = df_tracks_exploded["artists"].apply(lambda x: x.get("id"))


df_tracks_exploded["artists"] = artist_ids
df_tracks_exploded.columns = ["artist_id", "track_id"]

tracks_artists = df_tracks_exploded.drop_duplicates().dropna()

# cast and save track-artists
tracks_artists = tracks_artists.astype(
    {"track_id": pd.StringDtype(), "artist_id": pd.StringDtype()}
)
tracks_artists.to_parquet(f"{OUTPUT_PATH}/track_artist.parquet", index=False)

# get tracks_playlists relationship
now = time.perf_counter()
print("getting track playlists", now - start)
df_tracks_playlists = df_tracks[["id", "playlist_id"]].drop_duplicates().dropna()
df_tracks_playlists.columns = ["track_id", "playlist_id"]


# save tracks_playlists
df_tracks_playlists = df_tracks_playlists.astype(
    {"track_id": pd.StringDtype(), "playlist_id": pd.StringDtype()}
)
df_tracks_playlists.to_parquet(f"{OUTPUT_PATH}/track_playlist.parquet", index=False)

# drop artist id and playlist id now that we have the edge table? and drop duplicates for tracks
df_tracks = df_tracks.drop(columns=["playlist_id", "artists"])
df_tracks = df_tracks.drop_duplicates()


# process track features
now = time.perf_counter()
print("getting track features", now - start)
tracks_features = file_reader.read_files(INPUT_PATH, "track_features")
tracks_features = [i for i in tracks_features if i]
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

# save tracks with features
df_tracks_and_features = df_tracks_and_features.astype(
    {
        "id": pd.StringDtype(),
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
df_tracks_and_features.to_parquet(f"{OUTPUT_PATH}/tracks.parquet", index=False)

end = time.perf_counter()
print("total-time", now - start)
