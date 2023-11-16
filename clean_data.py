import pandas as pd
import numpy
import json

# all the relationship things we need to take care of
track_artists = []
track_playlists = []
album_artists = []
# track_genres = []

# this is where we need to basically get everything into our data model I think? atleast we need to deduplicate stuff
# df = pd.read_json("outputs/tracks.json")

# df.head()
# df.columns

# df_tracks = pd.json_normalize(df, "track")

# process tracks

with open("outputs/tracks.json", "r") as file:
    tracks = json.load(file)

# actual_tracks = [i.get("track") for i in tracks if i.get("track")]
# actual_tracks[0].keys()

df_tracks = pd.json_normalize(tracks)
df_tracks.columns

top_columns = [
    "id",
    "name",
    "popularity",
    "playlist_id",
    "album.id",
    "artists.id",
    "external_ids.isrc",
]

df_exploded = df_tracks[["artists", "id"]].explode(column="artists")
df_exploded.head(10)

artist_ids = df_exploded["artists"].apply(lambda x: x.get("id")).reset_index(drop=True)

len(artist_ids)

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
    "track_href",
]

df_features = df_features[top_columns]

# rename id to track_id
df_features = df_features.rename(columns={"id": "track_id"})

# actually were just gonna add all the analysis features to the track table because
# its only relation is to the track and contains all the metrics. without doing this
# tracks don't have manyfacts and the analysis doesn't have much reason for existing
