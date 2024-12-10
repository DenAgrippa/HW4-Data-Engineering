import sqlite3
import json

def read_text(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = f.read()
        data_entries = data.strip().split("=====")

        int_props = ['id', 'tours_count', 'min_rating', 'time_on_game', 'place', 'prise']
        items = []
        for entry in data_entries:
            if entry == '': continue

            item = {}
            entry_split = entry.strip().split('\n')
            for prop in entry_split:
                key_value_pair = prop.split('::')
                if key_value_pair[0] in int_props:
                    item[key_value_pair[0]] = int(key_value_pair[1])
                else:
                    item[key_value_pair[0]] = key_value_pair[1]
            items.append(item)
        
        return items


def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn


def create_tournament_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE tournament (
                   id integer primary key, 
                   name text, 
                   city text, 
                   begin text, 
                   system text, 
                   tours_count integer, 
                   min_rating integer, 
                   time_on_game integer)
    """)


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
           INSERT INTO tournament (id, name, city, begin, system, tours_count, min_rating, time_on_game)       
           VALUES (:id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game)
    """, items)
    db.commit()

def first_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM tournament
        ORDER BY min_rating
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
            COUNT(*) as tournament_count,
            MIN(tours_count) as min_tours_count,
            MAX(tours_count) as max_tours_count,
            ROUND(AVG(min_rating), 2) as avg_rating
        FROM tournament
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
            system
        FROM tournament
        GROUP BY system
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 


def fourth_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM tournament
        WHERE min_rating < 2300
        ORDER BY min_rating DESC
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 



# Step 1
create_tournament_table(connect_to_db("first.db"))

# Step 2
items = read_text("./data/1-2/item.text")
db = connect_to_db("first.db")
insert_data(db, items)

# Step 3.1
with open("./results/1/output_item.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)

# Step 3.2
with open("./results/1/second_querry_item.json", "w", encoding="utf-8") as f:
    json.dump(second_querry(db), f)

# Step 3.3
with open("./results/1/third_querry_item.json", "w", encoding="utf-8") as f:
    json.dump(third_querry(db), f)

# Step 3.4
with open("./results/1/fourth_querry_item.json", "w", encoding="utf-8") as f:
    json.dump(fourth_querry(db), f)