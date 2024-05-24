import sqlite3
import pandas as pd

con = sqlite3.connect("globant.db")
cur = con.cursor()

res = cur.execute("DROP TABLE IF EXISTS departments")
res = cur.execute("DROP TABLE IF EXISTS jobs")
res = cur.execute("DROP TABLE IF EXISTS hired_employees")
cur.execute("CREATE TABLE departments( id INTEGER PRIMARY KEY, department TEXT)")
cur.execute("CREATE TABLE jobs( id INT PRIMARY KEY, job VARCHAR(500))")
cur.execute("CREATE TABLE hired_employees( id INT PRIMARY KEY, name TEXT, datetime TEXT, department_id INTEGER, job_id INTEGER)")

departments_df = pd.read_csv('C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\departments.csv',sep=',', header=0)
departments_df.to_sql("departments",con,if_exists="replace")

jobs_df = pd.read_csv('C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\jobs.csv',sep=',', header=0)
jobs_df.to_sql("jobs",con,if_exists="replace")

hired_employees_df = pd.read_csv('C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\hired_employees.csv',sep=',', header=0)
hired_employees_df.to_sql("hired_employees",con,if_exists="replace")


res = cur.execute("SELECT * FROM departments limit 10")
res.fetchone()
con.close()
