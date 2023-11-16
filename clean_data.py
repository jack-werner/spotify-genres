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

with open("old_outputs/tracks.json", "r") as file:
    tracks = json.load(file)

actual_tracks = [i.get("track") for i in tracks if i.get("track")]
actual_tracks[0].keys()

df_new_tracks = pd.DataFrame(actual_tracks)
df_new_tracks.columns

top_columns = ["id", "name", "popularity"]

tracks[0].keys()

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
