import time
import pandas as pd
from pymongo import MongoClient
import csv

client = MongoClient("mongodb://localhost:27017")
db = client["steam_db"]
games_collection = db["games"]

def insert_from_csv():
    df = pd.read_csv("dataset/games.csv")  
    data = df.to_dict(orient="records")
    games_collection.delete_many({})  
    global start_insert, end_insert
    start_insert = time.time()
    games_collection.insert_many(data)
    end_insert = time.time()
    print(f"🟢 Inserção em massa: {end_insert - start_insert:.4f} segundos")

def simple_query():
    global start_simple, end_simple
    start_simple = time.time()
    list(games_collection.find({"price": {"$lt": 20}}))
    end_simple = time.time()
    print(f"🔵 Consulta simples: {end_simple - start_simple:.4f} segundos")

def complex_query():
    global start_complex, end_complex
    start_complex = time.time()
    list(games_collection.find(
        {"genres": "Action", "price": {"$lt": 30}},
        {"name": 1, "price": 1, "_id": 0}
    ).sort("price", 1))
    end_complex = time.time()
    print(f"🟣 Consulta complexa: {end_complex - start_complex:.4f} segundos")

def update_data():
    global start_update, end_update
    start_update = time.time()
    games_collection.update_many(
        {"genres": "RPG"},
        {"$inc": {"price": 2}}
    )
    end_update = time.time()
    print(f"🟠 Atualização: {end_update - start_update:.4f} segundos")

def delete_data():
    global start_delete, end_delete
    start_delete = time.time()
    games_collection.delete_many({"price": {"$gt": 100}})
    end_delete = time.time()
    print(f"🔴 Deleção: {end_delete - start_delete:.4f} segundos")

insert_from_csv()
simple_query()
complex_query()
update_data()
delete_data()

results = [
    ["Operação", "Tempo (segundos)"],
    ["Inserção em massa", end_insert - start_insert],
    ["Consulta simples", end_simple - start_simple],
    ["Consulta complexa", end_complex - start_complex],
    ["Atualização", end_update - start_update],
    ["Deleção", end_delete - start_delete],
]

with open("mongo/results.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(results)

print("✅ Resultados salvos no arquivo 'results.csv'")
