import sqlite3
import pickle
import json

def read_pkl(filename):
    with open(filename, "rb") as f:
        data = pickle.load(f)
    clean_data = []
    for item in data:
        if 'category' not in item: continue
        else: clean_data.append(item)
    return clean_data

def read_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_product_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE product (
                   id integer primary key, 
                   name text, 
                   price real, 
                   quantity integer, 
                   category text, 
                   fromCity text, 
                   isAvailable integer, 
                   views integer,
                   version integer default 0)
    """)

def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
           INSERT INTO product (name, price, quantity, category, fromCity, isAvailable, views)       
           VALUES (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
    """, items)
    db.commit()    

def handle_remove(db, name):
    cursor = db.cursor()
    cursor.execute("""
        DELETE FROM product WHERE name=?
    """, [name])
    db.commit()

def handle_price_percent(db, name, param):
    cursor = db.cursor()
    cursor.execute("UPDATE product SET price = ROUND(price * ( 1 + ?), 2), version = version + 1 WHERE name = ?", [param, name])
    db.commit()

def handle_price_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("UPDATE product SET price = price + ?, version = version + 1 WHERE name = ?", [param, name])
    db.commit()

def handle_quantity_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("UPDATE product SET quantity = quantity + ?, version = version + 1 WHERE name = ?", [param, name])
    db.commit()

def handle_availability(db, name, param):
    cursor = db.cursor()
    cursor.execute("UPDATE product SET isAvailable = ?, version = version + 1 WHERE name = ?", [param, name])
    db.commit()

def update_data(db, updates):
    for update in updates:
        if update['method'] == 'remove':
            handle_remove(db, update['name'])
        elif update['method'] == 'price_percent':
            handle_price_percent(db, update['name'], update['param'])
        elif update['method'] == 'price_abs':
            handle_price_abs(db, update['name'], update['param'])
        elif update['method'] == 'quantity_add':
            handle_quantity_abs(db, update['name'], update['param'])
        elif update['method'] == 'quantity_sub':
            handle_quantity_abs(db, update['name'], update['param'])
        elif update['method'] == 'available':
            handle_availability(db, update['name'], update['param'])

def first_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM product
        ORDER BY version DESC
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
            COUNT(*) as count,
            SUM(price) as sum_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            ROUND(AVG(price), 2) as avg_price,
            category
        FROM product
        GROUP BY category
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def third_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            COUNT(*) as count,
            SUM(quantity) as sum_quantity,
            MIN(quantity) as min_quantity,
            MAX(quantity) as max_quantity,
            ROUND(AVG(quantity), 2) as avg_quantity,
            category
        FROM product
        GROUP BY category
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items

def fourth_querry(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM product
        WHERE category = 'fruit' AND price < 60
        ORDER BY views DESC
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

# STEP 1
db = connect_to_db("fourth.db")
create_product_table(db)

# STEP 2
insert_data(db, read_pkl("./data/4/_product_data.pkl"))

# STEP 3
update_data(db, read_json("./data/4/_update_data.json"))

# STEP 4.1
with open("./results/4/first_q.json", "w", encoding="utf-8") as f:
    json.dump(first_querry(db), f)

# STEP 4.2
with open("./results/4/second_q.json", "w", encoding="utf-8") as f:
    json.dump(second_querry(db), f)

# STEP 4.3
with open("./results/4/third_q.json", "w", encoding="utf-8") as f:
    json.dump(third_querry(db), f)

# STEP 4.4
with open("./results/4/fourth_q.json", "w", encoding="utf-8") as f:
    json.dump(fourth_querry(db), f)