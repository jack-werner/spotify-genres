import pandas as pd

# import all data

genres = pd.read_csv("genres.csv")

artists = pd.read_parquet("artists.parquet", engine="pyarrow")
albums = pd.read_parquet("albums.parquet", engine="pyarrow")
tracks = pd.read_parquet("tracks.parquet", engine="pyarrow")
playlists = pd.read_parquet("playlists.parquet", engine="pyarrow")

playlist_genre = pd.read_parquet("playlist_genre.parquet", engine="pyarrow")
track_playlist = pd.read_parquet("track_playlist.parquet", engine="pyarrow")
track_artist = pd.read_parquet("track_artist.parquet", engine="pyarrow")
album_artist = pd.read_parquet("album_artists.parquet", engine="pyarrow")

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

playlists.head()

df3 = (
    df2.merge(playlist_genre, on="playlist_id")
    .merge(genres, left_on="genre_id", right_on="id")
    .drop(columns=["id"])
    .rename(columns={"name": "genre_name"})
)

track_counts = (
    df3.groupby(["genre_name", "track_name"]).count()["track_id"].reset_index()
)

# track_counts.sort_values(by=['genre_name', 'DistinctCount'], ascending=[True, False])
sorted_track_counts = track_counts.sort_values(
    by=["genre_name", "track_id"], ascending=[True, False]
)
top_3_songs = sorted_track_counts.groupby(by=["genre_name", "track_name"]).head(3)


# get most popular artists per genre


# get most popular albums per genre

# get avg and variance of each genre
