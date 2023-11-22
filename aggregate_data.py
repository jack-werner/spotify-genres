import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# import all data
INPUT_PATH = "silver"

genres = pd.read_csv("genres.csv")

artists = pd.read_parquet(f"{INPUT_PATH}/artists.parquet", engine="pyarrow")
albums = pd.read_parquet(f"{INPUT_PATH}/albums.parquet", engine="pyarrow")
tracks = pd.read_parquet(f"{INPUT_PATH}/tracks.parquet", engine="pyarrow")
playlists = pd.read_parquet(f"{INPUT_PATH}/playlists.parquet", engine="pyarrow")

playlist_genre = pd.read_parquet(
    f"{INPUT_PATH}/playlist_genre.parquet", engine="pyarrow"
)
track_playlist = pd.read_parquet(
    f"{INPUT_PATH}/track_playlist.parquet", engine="pyarrow"
)
track_artist = pd.read_parquet(f"{INPUT_PATH}/track_artist.parquet", engine="pyarrow")
album_artist = pd.read_parquet(f"{INPUT_PATH}/album_artists.parquet", engine="pyarrow")

# get most popular tracks per genre
df = tracks.merge(track_playlist, left_on="id", right_on="track_id").drop(
    columns="track_id"
)

df2 = (
    df.merge(
        playlists,
        left_on="playlist_id",
        right_on="id",
        suffixes=("_track", "_playlist"),
    )
    .drop(columns=["id_playlist"])
    .rename(
        columns={
            "id_track": "track_id",
            "name_track": "track_name",
            "name_playlist": "playlist_name",
        }
    )
)

df3 = (
    df2.merge(playlist_genre, on="playlist_id")
    .merge(genres, left_on="genre_id", right_on="id")
    .drop(columns=["id"])
    .rename(columns={"name": "genre_name"})
)

track_counts = (
    df3.groupby(["genre_name", "track_name", "track_id", "album_id"])
    .count()["playlist_id"]
    .reset_index()
).rename(columns={"playlist_id": "count"})

sorted_track_counts = track_counts.sort_values(
    by=["genre_name", "count"], ascending=[True, False]
).reset_index(drop=True)


top_songs = (
    sorted_track_counts.groupby(by=["genre_name"]).head(50).reset_index(drop=True)
)

# join artists
top_songs_artists = (
    top_songs.merge(track_artist, on="track_id")
    .merge(artists, left_on="artist_id", right_on="id", suffixes=("_track", "_artist"))
    .rename(columns={"name": "artist_name"})[
        ["genre_name", "track_name", "artist_name", "count"]
    ]
    .drop_duplicates()
    .sort_values(["genre_name", "count", "track_name"], ascending=[True, False, True])
).reset_index(drop=True)


top_songs_artists.to_csv("gold/top_50_songs.csv", index=False)

# get most popular artists per genre
songs_artists = track_counts.shape
track_counts_with_artists = (
    track_counts.merge(track_artist, on="track_id")
    .merge(artists, left_on="artist_id", right_on="id", suffixes=("_track", "_artist"))
    .rename(columns={"name": "artist_name"})
    .drop_duplicates()
)
multiple_appearances = track_counts_with_artists[track_counts_with_artists["count"] > 1]
sum_count = (
    multiple_appearances.groupby(["genre_name", "artist_name"])
    .sum()["count"]
    .reset_index()
)

sorted_artists = sum_count.sort_values(
    by=["genre_name", "count"], ascending=[True, False]
)
top_artists = sorted_artists.groupby("genre_name").head(20)
top_artists = top_artists.rename(columns={"count": "total_count"})
top_artists.to_csv("gold/top_20_artists.csv", index=False)


# get most popular albums per genre
track_counts_with_albums = (
    track_counts.merge(
        albums, left_on="album_id", right_on="id", suffixes=("_track", "_album")
    )
    .drop(columns=["id"])
    .rename(columns={"name": "album_name", "type": "album_type"})
)
track_counts_with_albums = track_counts_with_albums[
    track_counts_with_albums["count"] > 1
]
album_counts = (
    track_counts_with_albums.groupby(["genre_name", "album_name", "album_id"])["count"]
    .sum()
    .reset_index(name="total_count")
)
album_counts = album_counts.sort_values(
    ["genre_name", "total_count"], ascending=[True, False]
)

top_albums = album_counts.groupby("genre_name").head(20).reset_index(drop=True)

# join artists
top_albums_artists = (
    top_albums.merge(album_artist, on="album_id")
    .merge(artists, left_on="artist_id", right_on="id")
    .rename(columns={"name": "artist_name"})[
        ["genre_name", "album_name", "artist_name", "total_count"]
    ]
    .drop_duplicates()
    .sort_values(["genre_name", "total_count"], ascending=[True, False])
)


top_albums_artists.to_csv("gold/top_20_albums.csv", index=False)


# get track feature distributions
continuous_cols = [
    "popularity",
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "speechiness",
    "liveness",
    "loudness",
    "tempo",
    "valence",
    "duration_ms",
]

for col in continuous_cols:
    sns.set(rc={"figure.figsize": (15, 10)})
    plt.figure()
    ax = sns.boxplot(x="genre_name", y=col, data=df3)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    plt.subplots_adjust(left=0.05, right=0.98, top=0.9, bottom=0.2, wspace=0.4)
    plt.savefig(f"visualizations/{col}.png")
