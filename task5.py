import json
import csv
import sqlite3

def read_json(filename, source):
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    selected_data = []
    for entry in data[source]:
        if source == "bookingHotels":
            if len(entry["highlights"]) == 1: continue
            if entry["rating"]["score"] == 'No rating': continue
            item = {
                'title': entry["title"],
                'bed': entry["highlights"][1],
                'price': int(entry["price"]['value']),
                'period': "night",
                'rating': float(entry["rating"]["score"]),
            }
            selected_data.append(item)
        if source == "airbnbHotels":
            if entry["rating"] == "No rating": continue
            item = {
                'title': entry["title"],
                'bed': entry["subtitles"][1],
                'price': int(entry["price"]['value']),
                'period': entry["price"]['period'],
                'rating': float(entry["rating"]),
            }
            selected_data.append(item)
        if source == "hotelsComHotels":
            item = {
                'title': entry["title"],
                'price': entry["price"]['value'],
                'period': "night",
                'rating': entry["rating"]['score'],
            }
            selected_data.append(item)

    return selected_data

def read_csv(filename, source):
    items = []
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        reader.__next__()
        for row in reader:
            if len(row) == 0: continue
            if source == 'airbnbHotels':
                if row[8] == 'No rating': continue
                if row[6] == '': continue
                item = {
                    'title': row[1],
                    'bed': row[3],
                    'price': int(row[6]),
                    'period': row[7],
                    'rating': float(row[8]),
                }
            if source == "bookingHotels":
#               if row[22] == '': continue
                if row[24] == 'No rating': continue
                item = {
                    'title': row[11],
                    'bed': row[19],
                    'price': int(row[22]),
                    'period': "night",
                    'rating': float(row[24]),
                }           
            if source == "hotelsComHotels":
                item = {
                    'title': row[28],
                    'price': int(row[34]),
                    'period': "night",
                    'rating': float(row[36]),
                }  
            items.append(item)
    
    return items

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_hotel_table(db):
    cursor =  db.cursor()

    cursor.execute("""
        CREATE TABLE hotel (
                   id integer primary key,
                   title text,
                   name text,
                   bed text,
                   price integer,
                   period text,
                   rating real)
    """)

print(read_csv("data/5/London.csv", "bookingHotels"))