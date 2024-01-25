import psycopg2
import pandas as pd
import os
import json

#path = sys.argv[1]
path = "/home/tanveer/projects/paintings/data/"
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

    break

for i,j in zip(columns, dataTypes):
    print(i, j)


