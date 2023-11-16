import pandas as pd
import numpy
import json

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
