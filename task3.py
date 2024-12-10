import sqlite3
import csv
import json

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def read_csv(filename):
    items = []
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        reader.__next__()
        for row in reader:
            if len(row) == 0: continue
            item = {
                'artist': row[0],
                'song': row[1],
                'duration_ms': int(row[2]),
                'year': int(row[3]),
                'tempo': float(row[4]),
                'genre': row[5],
#                'energy': float(row[6]),
#                'key': int(row[7]),
                'loudness': float(row[8])
            }
            items.append(item)
    return items

def read_text(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = f.read()
        data_entries = data.strip().split("=====")

        int_props = ['duration', 'year']
        float_props = ['tempo', 'loudness']
        skip = ['instrumentalness', 'explicit']
        items = []
        for entry in data_entries:
            if entry == '': continue

            item = {}
            entry_split = entry.strip().split('\n')
            for prop in entry_split:
                key_value_pair = prop.split('::')
                if key_value_pair[0] in skip: continue
                if key_value_pair[0] in int_props:
                    item[key_value_pair[0]] = int(key_value_pair[1])
                elif key_value_pair[0] in float_props:
                    item[key_value_pair[0]] = float(key_value_pair[1])
                else:
                    item[key_value_pair[0]] = key_value_pair[1]
            items.append(item)
        
        return items
    
def create_music_table(db):
    cursor =  db.cursor()

    cursor.execute("""
        CREATE TABLE music (
                   id integer primary key,
                   artist text,
                   song text,
                   duration_ms integer,
                   year integer,
                   tempo real,
                   genre text,
                   loudness real)
    """)

def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO music (artist, song, duration_ms, year, tempo, genre, loudness)
        VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :loudness)
    """, items)
    db.commit()

def first_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM music
        ORDER BY year
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items    

def second_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as song_count,
            MIN(duration_ms) as min_duration,
            MAX(duration_ms) as max_duration,
            ROUND(AVG(tempo), 2) as avg_tempo
        FROM music
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items[0]

def third_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as count,
            genre
        FROM music
        GROUP BY genre
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

def fourth_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM music
        WHERE genre = 'metal'
        ORDER BY loudness DESC
        LIMIT 15
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 



# Step 1
db = connect_to_db("second.db")
create_music_table(db)

# Step 2
insert_data(db, read_csv("./data/3/_part_1.csv") + read_text("./data/3/_part_2.text"))

# STEP 3.1
with open("./results/3/first_q.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)

# STEP 3.2
with open("./results/3/second_q.json", "w", encoding="utf-8") as f:
    json.dump(second_querry(db), f)

# STEP 3.3
with open("./results/3/third_q.json", "w", encoding="utf-8") as f:
    json.dump(third_querry(db), f)

# STEP 3.4
with open("./results/3/fourth_q.json", "w", encoding="utf-8") as f:
    json.dump(fourth_querry(db), f)