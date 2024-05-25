import sqlite3
import pandas as pd

con = sqlite3.connect("globant.db")
cur = con.cursor()

res = cur.execute("DROP TABLE IF EXISTS departments")
res = cur.execute("DROP TABLE IF EXISTS jobs")
res = cur.execute("DROP TABLE IF EXISTS hired_employees")
cur.execute("CREATE TABLE departments( id INTEGER PRIMARY KEY, department TEXT)")
cur.execute("CREATE TABLE jobs( id INTEGER PRIMARY KEY, job TEXT)")
cur.execute("CREATE TABLE hired_employees( id INTEGER PRIMARY KEY, name TEXT, datetime TEXT, department_id INTEGER, job_id INTEGER)")

con.close()
