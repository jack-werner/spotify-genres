import os
import json


def read_files(parent_directory: str, file_name: str):
    content = []
    entries = os.listdir(parent_directory)
    directories = [
        entry
        for entry in entries
        if os.path.isdir(os.path.join(parent_directory, entry))
    ]
    for directory in directories:
        try:
            path = f"{parent_directory}/{directory}/{file_name}.json"
            with open(path, encoding="utf-8") as file:
                content += json.load(file)
        except FileNotFoundError:
            print(f"File: '{path}' not found, continuing.")
    return content


def handle_dates(row):
    if row["release_date_precision"] == "year":
        return f"{row['release_date']}-01-01"
    elif row["release_date_precision"] == "month":
        return f"{row['release_date']}-01"
    else:
        return row["release_date"]
