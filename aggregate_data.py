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

track_counts = (
    df3.groupby(["genre_name", "track_name", "track_id"])
    .count()["album_id"]
    .reset_index()
).rename(columns={"album_id": "count"})


# track_counts.sort_values(by=['genre_name', 'DistinctCount'], ascending=[True, False])
sorted_track_counts = track_counts.sort_values(
    by=["genre_name", "count"], ascending=[True, False]
)


top_songs = (
    sorted_track_counts.groupby(by=["genre_name"]).head(10).reset_index(drop=True)
)

top_songs.head(25)

# join artists
top_songs_artists = (
    top_songs.merge(track_artist, on="track_id")
    .merge(artists, left_on="artist_id", right_on="id", suffixes=("_track", "_artist"))
    .rename(columns={"name": "artist_name"})[
        ["genre_name", "track_name", "artist_name", "count"]
    ]
    .drop_duplicates()
)


top_songs_artists[top_songs_artists["genre_name"] == "power-pop"]
top_songs_artists[top_songs_artists["genre_name"] == "alt-rock"]
top_songs_artists[top_songs_artists["genre_name"] == "grunge"]
top_songs_artists[top_songs_artists["genre_name"] == "dubstep"]
top_songs_artists[top_songs_artists["genre_name"] == "dub"]


# get most popular artists per genre


# get most popular albums per genre

# get avg and variance of each genre
