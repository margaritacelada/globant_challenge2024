from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
import pandas as pd

# Class Department
class Department(BaseModel):
    id: int
    department: str

# Class Department
class Job(BaseModel):
    id: int
    job: str

# Class Department
class Hired_Employee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

#[]

app = FastAPI()

@app.post("/load_table")
async def load_table(file:str):
    con = sqlite3.connect("globant.db")
    cur = con.cursor()
    df = pd.read_csv(f'C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\{file}.csv',sep=',', header=0)
    df = df.loc[:1000,:]
    tuples = [tuple(x) for x in df.to_numpy() ]
    cur.executemany(f"INSERT INTO {file} VALUES(?, ?)", tuples)
    con.commit()
    res = cur.execute(f"SELECT * FROM {file} limit 10")
    return res.fetchall()


