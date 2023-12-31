import pandas as pd
import os
import json
from spotify_scraper import SpotifyExtractor

OUTPUT_PATH = "bronze"


def save_file(content: list[dict], filename: str, genre: str):
    output_path = f"{OUTPUT_PATH}/{genre}/"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    with open(f"{output_path}/{filename}.json", "w", encoding="utf-8") as file:
        json.dump(content, file)


spotify = SpotifyExtractor(timeout=20)

# get genres
df_genres = pd.read_csv("genres.csv")
genres = df_genres["name"].to_list()

# for genre in genres:
for genre in genres:
    # get playlists
    playlists = spotify.get_all_playlists(genre)
    save_file(playlists, "playlists", genre)
    # get songs
    tracks = spotify.get_all_songs_from_playlists(playlists, limit=50, delay=1)
    save_file(tracks, "tracks", genre)
    # get features
    track_ids = [i.get("id") for i in tracks if i.get("id")]
    track_ids = list(set(track_ids))
    track_features = spotify.get_all_track_features(track_ids, limit=100)
    save_file(track_features, "track_features", genre)
    # get artists
    artist_lists = [i.get("artists") for i in tracks if i.get("artists")]
    artist_ids = []
    for artist_list in artist_lists:
        artists_ids = [i.get("id") for i in artist_list if i.get("id")]
        artist_ids += artists_ids
    artist_ids = list(set(artist_ids))
    artists = spotify.get_all_artists(artist_ids, limit=50)
    save_file(artists, "artists", genre)
    # get albums
    accessed_albums = [i.get("album") for i in tracks if i.get("album")]
    album_ids = [i.get("id") for i in accessed_albums if i.get("id")]
    album_ids = list(set(album_ids))
    albums = spotify.get_all_albums(album_ids, limit=20)
    save_file(albums, "albums", genre)
