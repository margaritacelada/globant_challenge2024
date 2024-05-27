import sqlite3
import pandas as pd

con = sqlite3.connect("globant.db")
cur = con.cursor()

cur.execute("DROP TABLE IF EXISTS departments")
cur.execute("DROP TABLE IF EXISTS jobs")
cur.execute("DROP TABLE IF EXISTS hired_employees")
cur.execute("CREATE TABLE departments( id INTEGER PRIMARY KEY, department VARCHAR(100) NOT NULL)")
cur.execute("CREATE TABLE jobs( id INTEGER PRIMARY KEY, job VARCHAR(100) NOT NULL)")
cur.execute("CREATE TABLE hired_employees( id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, datetime VARCHAR(100) NOT NULL, department_id INTEGER NOT NULL, job_id INTEGER NOT NULL)")

con.close()
