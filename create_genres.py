import pandas as pd
import uuid

file_path = "limited-genres.txt"
with open(file_path, "r") as file:
    content_list = file.readlines()

genres = [line.strip() for line in content_list]

df_genre = pd.DataFrame(genres, columns=["name"])
df_genre["id"] = [uuid.uuid4() for _ in range(df_genre.shape[0])]
df_genre = df_genre[["id", "name"]]

# save
df_genre.to_csv("genres.csv", index=False)
