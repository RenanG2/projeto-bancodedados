import time
import pandas as pd
import mysql.connector
import csv
import os

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1302"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS steam_db_mysql")
cursor.execute("USE steam_db_mysql")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        appid INT PRIMARY KEY,
        name VARCHAR(255),
        release_date VARCHAR(255),
        english TINYINT,
        developer VARCHAR(255),
        publisher VARCHAR(255),
        platforms VARCHAR(255),
        required_age INT,
        categories TEXT,
        genres TEXT,
        steamspy_tags TEXT,
        achievements INT,
        positive_ratings INT,
        negative_ratings INT,
        average_playtime INT,
        median_playtime INT,
        owners VARCHAR(50),
        price FLOAT
    )
""")

def insert_from_csv():
    df = pd.read_csv("dataset/games.csv")  
    data = df.values.tolist()
    cursor.execute("DELETE FROM games")
    conn.commit()
    
    start = time.time()
    for row in data:
        try:
            cursor.execute("""
                INSERT INTO games VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, row)
        except:
            continue
    conn.commit()
    end = time.time()
    print(f"🟢 Inserção em massa: {end - start:.4f} segundos")
    return end - start

def simple_query():
    start = time.time()
    cursor.execute("SELECT * FROM games WHERE price < 20")
    cursor.fetchall()
    end = time.time()
    print(f"🔵 Consulta simples: {end - start:.4f} segundos")
    return end - start

def complex_query():
    start = time.time()
    cursor.execute("SELECT name, price FROM games WHERE genres LIKE '%Action%' AND price < 30 ORDER BY price ASC")
    cursor.fetchall()
    end = time.time()
    print(f"🟣 Consulta complexa: {end - start:.4f} segundos")
    return end - start

def update_data():
    start = time.time()
    cursor.execute("UPDATE games SET price = price + 2 WHERE genres LIKE '%RPG%'")
    conn.commit()
    end = time.time()
    print(f"🟠 Atualização: {end - start:.4f} segundos")
    return end - start

def delete_data():
    start = time.time()
    cursor.execute("DELETE FROM games WHERE price > 100")
    conn.commit()
    end = time.time()
    print(f"🔴 Deleção: {end - start:.4f} segundos")
    return end - start

times = [
    ["Operação", "Tempo (segundos)"],
    ["Inserção em massa", insert_from_csv()],
    ["Consulta simples", simple_query()],
    ["Consulta complexa", complex_query()],
    ["Atualização", update_data()],
    ["Deleção", delete_data()]
]

with open("results.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(times)

print("✅ Resultados salvos em sql/results.csv")

cursor.close()
conn.close()
