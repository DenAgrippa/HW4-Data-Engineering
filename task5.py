import sqlite3
import pandas as pd
import json

# Содержит местоположение и информацию о каждом дорожно-транспортном происшествии, 
# зарегистрированном в полиции округа Вашингтон с 2011 по 2015 год. 
# Поля включают тяжесть травм, количество погибших, информацию об участвовавших транспортных средствах, 
# информацию о местоположении и факторы, которые могли способствовать аварии.
#
# https://www.kaggle.com/datasets/mahdiehhajian/washington-county-crash-data/data
def read_csv(file_path):
    return pd.read_csv(file_path)

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_car_crash_table(db, table_name):
    cursor = db.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            CRASH_CRN int primary key,
            MUNICIPALITY int,
            POLICE_AGCY int,
            CRASH_YEAR int,
            HOUR_OF_DAY int,
            WEATHER int,
            ILLUMINATION int,
            ROAD_CONDITION int,
            COLLISION_TYPE int,
            FATAL_COUNT int,
            INJURY_COUNT int,
            MAX_SEVERITY_LEVEL int)
    """)

def create_dict_table(db, table_name):
    cursor = db.cursor()
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            code int primary key,
            description text)
    """)

def insert_crash_data(db, table_name, data):
    cursor = db.cursor()
    cursor.executemany(f"""
        INSERT INTO {table_name} (CRASH_CRN, MUNICIPALITY, POLICE_AGCY, CRASH_YEAR, HOUR_OF_DAY, WEATHER, ILLUMINATION, ROAD_CONDITION, COLLISION_TYPE, FATAL_COUNT, INJURY_COUNT, MAX_SEVERITY_LEVEL)
        VALUES (:CRASH_CRN, :MUNICIPALITY, :POLICE_AGCY, :CRASH_YEAR, :HOUR_OF_DAY, :WEATHER, :ILLUMINATION, :ROAD_CONDITION, :COLLISION_TYPE, :FATAL_COUNT, :INJURY_COUNT, :MAX_SEVERITY_LEVEL)
    """, data)
    db.commit()

def insert_dict_data(db, table_name, data):
    cursor = db.cursor()
    cursor.executemany(f"""
        INSERT INTO {table_name} (Code, Description)
        VALUES (:Code, :Description)
    """, data)
    db.commit()

def querry_1(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT MUNICIPALITY, WEATHER, MAX_SEVERITY_LEVEL
        FROM WA2014
        WHERE MAX_SEVERITY_LEVEL IS NOT NULL
        ORDER BY MAX_SEVERITY_LEVEL DESC
        LIMIT 5
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

def querry_2(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) AS Total_Crashes,
            SUM(FATAL_COUNT) AS Total_Fatalities,
            SUM(INJURY_COUNT) AS Total_Injuries
        FROM WA2015
        WHERE WEATHER = 3
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

def querry_3(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            WEATHER,
            COUNT(*) AS Total_Crashes,
            SUM(FATAL_COUNT) AS Total_Fatalities,
            SUM(INJURY_COUNT) AS Total_Injuries
        FROM WA2014
        GROUP BY WEATHER
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

def querry_4(db):
    cursor = db.cursor()
    res = cursor.execute("""
        UPDATE WA2014
        SET CRASH_YEAR = CRASH_YEAR + 1;
    """)
    db.commit()

def querry_5(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            m.Description AS Municipality_Name, 
            COUNT(w.CRASH_CRN) AS Crash_Count
        FROM WA2014 w
        JOIN municipal_codes m
            ON w.MUNICIPALITY = m.Code
        GROUP BY m.Description
        HAVING COUNT(w.CRASH_CRN) > 10
    """)
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    return items 

def write_to_json(data, task_number, file_id=''):
    with open(f'results/{task_number}/output_{file_id}.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

db = connect_to_db("car_crash_data.db")
create_car_crash_table(db, 'WA2014')
create_car_crash_table(db, 'WA2015')
create_dict_table(db, 'municipal_codes')
create_dict_table(db, 'police_codes')

load_list = ['CRASH_CRN',
            'MUNICIPALITY', 
            'POLICE_AGCY', 
            'CRASH_YEAR',
            'HOUR_OF_DAY',
            'WEATHER',
            'ILLUMINATION',
            'ROAD_CONDITION',
            'COLLISION_TYPE',
            'FATAL_COUNT',
            'INJURY_COUNT',
            'MAX_SEVERITY_LEVEL']

df_crash_2014 = pd.read_json('data/5/2014washington.json')[load_list].to_dict(orient='records')
df_crash_2015 = pd.read_csv('data/5/2015washington.csv')[load_list].to_dict(orient='records')
df_dict_municipal = pd.read_json('data/5/washingtonmunicipalcode.json').rename(columns= {'Municipality': 'Description'}).to_dict(orient='records')
df_dict_police = pd.read_json('data/5/washingtonpoliceagencycode.json').rename(columns= {'Policy Agency': 'Description'}).to_dict(orient='records')

insert_crash_data(db, 'WA2014', df_crash_2014)
insert_crash_data(db, 'WA2015', df_crash_2015)
insert_dict_data(db, 'municipal_codes', df_dict_municipal)
insert_dict_data(db, 'police_codes', df_dict_police)

write_to_json(querry_1(db), '5', '1')
write_to_json(querry_2(db), '5', '2')
write_to_json(querry_3(db), '5', '3')
querry_4(db)
write_to_json(querry_5(db), '5', '5')