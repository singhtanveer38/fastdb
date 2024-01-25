import psycopg2
import pandas as pd
import os
import json
import io

#path = sys.argv[1]
path = "/home/tanveer/downloads/MusicalCollaborations/"
files = os.listdir(path)
database = "yoyo"

tables = []
for file in files:
    if file.split(".")[1] == "csv":
        tables.append(file)

with open("cred.json", "r") as f:
    cred = json.load(f)

host = cred["postgres"]["host"]
port = cred["postgres"]["port"]
password = cred["postgres"]["password"]
user = cred["postgres"]["user"]

conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)

curr = conn.cursor()

for table in tables:
    df = pd.read_csv(path+table)
    columns = df.columns
    dataTypes = df.dtypes


    tableTypes = []
    for i,j in zip(columns, dataTypes):
        tableTypes.append(f"{i} {j}")

    print(tableTypes)

    tableTypes = ", ".join(tableTypes)

    tableTypes = tableTypes.replace("int64", "int")
    tableTypes = tableTypes.replace("object", "varchar")
    tableTypes = tableTypes.replace("float64", "float")

    print(tableTypes)

    curr.execute(f"create table {table.split('.')[0]} ({tableTypes});")

    with io.open(path+table, "r", encoding='utf-8') as f:
        next(f)
        curr.copy_from(f, table.split('.')[0], sep=',')


conn.commit()
conn.close()
