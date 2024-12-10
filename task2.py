import sqlite3
import json

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

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
    
def create_prise_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE prise (
                   id integer primary key, 
                   name text references tournament(name),
                   place integer,
                   prise integer)
    """)

def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
           INSERT INTO prise (name, place, prise)       
           VALUES (:name, :place, :prise)
    """, items)
    db.commit()

def first_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM prise
        WHERE name = 'Сент-Луис 1958'
        ORDER BY place
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def second_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT t.name, t.id, p.prise
        FROM tournament t
        JOIN prise p ON t.name = p.name
        WHERE p.place = 0
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def third_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT t.name, sum(p.prise) as prise_found, count(p.place) as prise_count
        FROM tournament t
        JOIN prise p ON t.name = p.name
        GROUP BY t.name
        ORDER BY prise_found DESC
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items



# Step 1
print(read_text("./data/1-2/subitem.text"))

# Step 2
db = connect_to_db("first.db")
create_prise_table(db)

# Step 3
insert_data(db, read_text("./data/1-2/subitem.text"))

# Step 4.1
with open("./results/2/first_q.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)

# Step 4.2
with open("./results/2/second_q.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)

# Step 4.3 
with open("./results/2/third_q.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)