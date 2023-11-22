import pandas as pd

# import all data
INPUT_PATH = "normalized"

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

# look at data size
artists.shape
albums.shape
tracks.shape
playlists.shape

playlist_genre.shape
track_playlist.shape
track_artist.shape
album_artist.shape

# track_playlist.columns

# get most popular tracks per genre

df = tracks.merge(track_playlist, left_on="id", right_on="track_id").drop(
    columns="track_id"
)
df.columns

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

# playlists.head()

df3 = (
    df2.merge(playlist_genre, on="playlist_id")
    .merge(genres, left_on="genre_id", right_on="id")
    .drop(columns=["id"])
    .rename(columns={"name": "genre_name"})
)

# merge artists
df4 = (
    df3.merge(track_artist, on="track_id")
    .merge(artists, left_on="artist_id", right_on="id", suffixes=("_track", "_artist"))
    .rename(
        columns={
            "popularity_track": "track_popularity",
            "popularity_artist": "artist_popularity",
            "id": "artist_id",
            "name": "artist_name",
        }
    )
)
df4.columns

# track_counts = (
#     df3.groupby(["genre_name", "track_name", "track_id"])
#     .count()["album_id"]
#     .reset_index()
# ).rename(columns={"album_id": "count"})

# track_counts = (
#     df3.groupby(by=["genre_name", "track_name", "track_id", "album_id"], as_index=False)
#     .value_counts()
#     .reset_index(name='count')
# )

track_counts = (
    df3.groupby(["genre_name", "track_name", "track_id", "album_id"])
    .count()["playlist_id"]
    .reset_index()
).rename(columns={"playlist_id": "count"})


# track_counts.sort_values(by=['genre_name', 'DistinctCount'], ascending=[True, False])
sorted_track_counts = track_counts.sort_values(
    by=["genre_name", "count"], ascending=[True, False]
).reset_index(drop=True)

sorted_track_counts[sorted_track_counts["genre_name"] == "grunge"]


top_songs = (
    sorted_track_counts.groupby(by=["genre_name"]).head(50).reset_index(drop=True)
)

top_songs.head(25)
top_songs[top_songs["genre_name"] == "alt-rock"]

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


top_songs_artists[top_songs_artists["genre_name"] == "power-pop"]
top_songs_artists[top_songs_artists["genre_name"] == "alt-rock"]
top_songs_artists[top_songs_artists["genre_name"] == "grunge"]
top_songs_artists[top_songs_artists["genre_name"] == "indie"]
top_songs_artists[top_songs_artists["genre_name"] == "dubstep"]
top_songs_artists[top_songs_artists["genre_name"] == "dub"]
top_songs_artists[top_songs_artists["genre_name"] == "opera"]
top_songs_artists[top_songs_artists["genre_name"] == "jazz"]
top_songs_artists[top_songs_artists["genre_name"] == "anime"]
top_songs_artists[top_songs_artists["genre_name"] == "synth-pop"]
top_songs_artists[top_songs_artists["genre_name"] == "trip-hop"]
top_songs_artists[top_songs_artists["genre_name"] == "ska"]
top_songs_artists[top_songs_artists["genre_name"] == "pop"].sort_values(
    "count", ascending=False
)


top_songs_artists.to_csv("gold/top_50_songs.csv", index=False)

# get most popular artists per genre
# artists with the most popular tracks in each genre
songs_artists = track_counts.shape
track_counts_with_artists = (
    track_counts.merge(track_artist, on="track_id")
    .merge(artists, left_on="artist_id", right_on="id", suffixes=("_track", "_artist"))
    .rename(columns={"name": "artist_name"})
    .drop_duplicates()
)

track_counts_with_artists.shape

track_counts_with_artists.head()
multiple_appearances = track_counts_with_artists[track_counts_with_artists["count"] > 1]

track_counts_with_artists.columns

sum_count = (
    multiple_appearances.groupby(["genre_name", "artist_name"])
    .sum()["count"]
    .reset_index()
)

sorted_artists = sum_count.sort_values(
    by=["genre_name", "count"], ascending=[True, False]
)
top_artists = sorted_artists.groupby("genre_name").head(20)

# removing the songs that only show up in one playlist seems to make this better
top_artists[top_artists["genre_name"] == "power-pop"]
top_artists[top_artists["genre_name"] == "pop"]
top_artists[top_artists["genre_name"] == "grunge"]
top_artists[top_artists["genre_name"] == "classical"]
top_artists[top_artists["genre_name"] == "dubstep"]
top_artists[top_artists["genre_name"] == "house"]
top_artists[top_artists["genre_name"] == "reggae"]
top_artists[top_artists["genre_name"] == "reggaeton"]
top_artists[top_artists["genre_name"] == "opera"]
top_artists[top_artists["genre_name"] == "emo"]
top_artists[top_artists["genre_name"] == "jazz"]
top_artists[top_artists["genre_name"] == "garage"]
top_artists[top_artists["genre_name"] == "edm"]
top_artists[top_artists["genre_name"] == "goth"]
top_artists[top_artists["genre_name"] == "country"]
top_artists[top_artists["genre_name"] == "idm"]
top_artists[top_artists["genre_name"] == "metal"]
top_artists[top_artists["genre_name"] == "synth-pop"]
top_artists[top_artists["genre_name"] == "hip-hop"]
top_artists[top_artists["genre_name"] == "ambient"]
top_artists[top_artists["genre_name"] == "techno"]
top_artists[top_artists["genre_name"] == "ska"]
top_artists[top_artists["genre_name"] == "alt-rock"]

top_artists = top_artists.rename(columns={"count": "total_count"})
top_artists.to_csv("gold/top_20_artists.csv", index=False)


# get most popular albums per genre
# join albums

track_counts_with_albums = (
    track_counts.merge(
        albums, left_on="album_id", right_on="id", suffixes=("_track", "_album")
    )
    .drop(columns=["id"])
    .rename(columns={"name": "album_name", "type": "album_type"})
)
track_counts_with_albums.columns

track_counts_with_albums = track_counts_with_albums[
    track_counts_with_albums["count"] > 1
]

album_counts = (
    track_counts_with_albums.groupby(["genre_name", "album_name", "album_id"])["count"]
    .sum()
    .reset_index(name="total_count")
)
album_counts.columns
album_counts = album_counts.sort_values(
    ["genre_name", "total_count"], ascending=[True, False]
)

top_albums = album_counts.groupby("genre_name").head(20).reset_index(drop=True)
top_albums


top_albums[top_albums["genre_name"] == "grunge"]
top_albums[top_albums["genre_name"] == "hip-hop"]
top_albums[top_albums["genre_name"] == "disco"]
top_albums[top_albums["genre_name"] == "indie"]
top_albums[top_albums["genre_name"] == "rock"]
top_albums[top_albums["genre_name"] == "hard-rock"]
top_albums[top_albums["genre_name"] == "ambient"]
top_albums[top_albums["genre_name"] == "metal"]
top_albums[top_albums["genre_name"] == "bossanova"]
top_albums[top_albums["genre_name"] == "synth-pop"]
top_albums[top_albums["genre_name"] == "idm"]
top_albums[top_albums["genre_name"] == "country"]
top_albums[top_albums["genre_name"] == "blues"]

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

top_albums_artists[top_albums_artists["genre_name"] == "grunge"]
top_albums_artists[top_albums_artists["genre_name"] == "hip-hop"]
top_albums_artists[top_albums_artists["genre_name"] == "disco"]
top_albums_artists[top_albums_artists["genre_name"] == "ambient"]
top_albums_artists[top_albums_artists["genre_name"] == "metal"]
top_albums_artists[top_albums_artists["genre_name"] == "country"]
top_albums_artists[top_albums_artists["genre_name"] == "rock"]
top_albums_artists[top_albums_artists["genre_name"] == "alt-rock"]
top_albums_artists[top_albums_artists["genre_name"] == "grunge"]


top_albums_artists.to_csv("gold/top_20_albums.csv", index=False)

########
# album_artist[album_artist["album_id"] == "2guirTSEqLizK7j9i1MTTZ"]

top_albums.shape
album_artist.shape

# top_albums_artists = top_albums_artists.drop_duplicates()


########


# get avg and variance of each genre

# join track to genre
df = tracks.merge(track_playlist, left_on="id", right_on="track_id").drop(
    columns="track_id"
)
df.columns

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

# playlists.head()

df3 = (
    df2.merge(playlist_genre, on="playlist_id")
    .merge(genres, left_on="genre_id", right_on="id")
    .drop(columns=["id"])
    .rename(columns={"name": "genre_name"})
)


import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# plt.figure(figsize=(15, 10))

df3.columns
continuous_cols = [
    "popularity",
    "acousticness",
    "danceability",
    "energy",
    "instrumentalness",
    "speechiness",
    # "key",
    "liveness",
    "loudness",
    # "mode",
    "tempo",
    # "time_signature",
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
# plt.show()


# Rotate the x-axis labels


fig, axes = plt.subplots(nrows=7, ncols=1, figsize=(10, 5))

df3["group"] = pd.cut(df3["genre_name"], bins=7)

# Create boxplots for two variables side by side
sns.boxplot(x="genre_name", y="popularity", data=df3)
sns.boxplot(x="species", y="sepal_width", data=iris, ax=axes[1])

plt.tight_layout()
plt.show()

genres_group = genres.reset_index()
genres_group["group"] = pd.cut(genres_group["index"], bins=7)

genres_group.head(20)
