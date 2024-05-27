from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import Optional
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import pandavro as pdv


FILE_ROUTE = 'C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\'

engine = create_engine('sqlite:///C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\globant.db', echo=False)

department_schema = {'id':'int32','department':'str'}
hired_employees_schema = {'id':'int32','name':'str','datetime':'str','department_id':'int32','job_id':'int32'}
jobs_schema = {'id':'int32','job':'str'}
allowed_tables = ["departments", "jobs", "hired_employees"]

class BatchInput(BaseModel):
    file_name: str
    table_name: str
    nrows: Optional[int] = 1000

class BackupInput(BaseModel):
    table_name: str

app_globant = FastAPI()

# Challenge 1
@app_globant.post("/batch_load")
async def batch_load(batch_arg:BatchInput):
    table_name = batch_arg.table_name
    file = batch_arg.file_name
    nrows_insert = batch_arg.nrows

    if table_name not in allowed_tables:
        return HTTPException(status_code=404, detail=f"This table {table_name} is not available")
    if  1 <= nrows_insert < 1000:
        return HTTPException(status_code=403, detail=f"Invalid batch number rows")

    file_name =  FILE_ROUTE + f'historical_data\\{file}.csv'

    if table_name == 'departments':
        table_schema = department_schema
    if table_name == 'jobs':
        table_schema = jobs_schema
    if table_name == 'hired_employees':
        table_schema = hired_employees_schema

    table_cols = list(table_schema.keys())
    df = pd.read_csv(file_name, sep=',', names=table_cols, nrows=nrows_insert)
    
    # historical records with missing values shouldn't be inserted
    df_nulls = df[df.isnull().any(axis=1)]
    id_nulls = df_nulls['id'].to_list()
    df.dropna(inplace=True)
    
    # Check input data has the column datatypes expected
    try:
        df = df.astype(table_schema)
    except:
        HTTPException(status_code=406, detail=f"input data for table {table_name} is not valid. Expected {table_schema} ")
    
    # historical records with duplicated ids shouldn't be inserted
    id_dup_mask = df.duplicated(subset=['id'], keep=False)
    id_dups = df.loc[id_dup_mask, 'id'].tolist()
    df.drop_duplicates(subset=['id'], inplace=True)

    # if there's data in the db, don't insert ids that already exist
    with engine.connect() as con:
        result = con.execute(text(f"SELECT id FROM {table_name}")).fetchall()
    id_exists = [x[0] for x in result]
    id_allowed = df["id"].to_list()
    id_no_allowed = []
    if len(id_exists) > 0:
        id_allowed =  list(set(df['id']).difference(set(id_exists)))
        id_no_allowed = list(set(df['id']).intersection(set(id_exists)))
        df = df[df["id"].isin(id_allowed)]
    
    # insert records in db
    df.drop(columns='index',errors='ignore',inplace=True)
    rows_inserted = df.to_sql(table_name, engine, if_exists='append',index=False)
    engine.connect().commit()

    # build the response
    response_end = f"""{rows_inserted} rows inserted",
            {len(id_nulls)} rows were not inserted due to nulls {','.join(str(x) for x in id_nulls)},
            {len(id_dups)} rows were not inserted due to duplicated ids in the source file {','.join(str(x) for x in id_dups)},
            {len(id_no_allowed)} rows were not inserted due to ids already exists in {table_name} table {','.join(str(x) for x in id_no_allowed)}
            """

    if rows_inserted > 0:
        response_header = "Batch load was successful "
    else:
        response_header = "Batch load was unsuccessful "

    response = response_header + response_end

    return response

@app_globant.post("/backup_table")
async def backup_table(backup_args:BackupInput):
    table_name = backup_args.table_name

    # Check table has data or exist
    with engine.connect() as con:
        result = con.execute(text(f"SELECT count(1) FROM {table_name}")).fetchall()
        
    if result is None or [x[0] for x in result][0] == 0: 
        raise HTTPException(status_code=404, detail=f"There's no data in {table_name} or table doesn't exist")

    # read data
    num_rows = [x[0] for x in result][0]
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    # save backup
    file_name =  FILE_ROUTE + f'bck\\{table_name}.avro'
    pdv.to_avro(file_name, df)

    response = f"The backup for table {table_name} was succesful, with {num_rows} rows saved"

    return response

@app_globant.post("/restore_table")
async def restore_table(backup_args:BackupInput):
    table_name = backup_args.table_name
    file_name =  FILE_ROUTE + f'bck\\{table_name}.avro'
    df = pdv.read_avro(file_name)
    
    with engine.connect() as con:
        con.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    rows_inserted = df.to_sql(table_name, engine, if_exists='append',index=False)
    engine.connect().commit()
    response = f"Table {table_name} succesfully restored with {rows_inserted} rows inserted"

    return response

# Challenge 2
@app_globant.get("/hired_employees_q")
async def hired_employees_q():
    
    # check if there's data
    for table in allowed_tables:
        with engine.connect() as con:
            result = con.execute(text(f"SELECT count(1) FROM {table}")).fetchall()
            if result is None or [x[0] for x in result][0] == 0: 
                raise HTTPException(status_code=404, detail=f"There's no data in {table} or table doesn't exist")
    
    # get metrics
    query = """
        with all_quarters AS (
            SELECT 
                d.department,
                j.job,
                CASE
                    WHEN strftime('%m', h.datetime) BETWEEN '01' AND '03' THEN 'Q1'
                    WHEN strftime('%m', h.datetime) BETWEEN '04' AND '06' THEN 'Q2'
                    WHEN strftime('%m', h.datetime) BETWEEN '07' AND '09' THEN 'Q3'
                    ELSE 'Q4'
                END AS quarter
            FROM hired_employees h
                JOIN departments d ON d.id = h.department_id
                JOIN jobs j ON j.id = h.job_id
            WHERE strftime('%Y', datetime) = '2021'
            )
            SELECT
                department,
                job,
                sum(CAST(quarter = 'Q1' as integer)) AS Q1,
                sum(CAST(quarter = 'Q2' as integer)) AS Q2,
                sum(CAST(quarter = 'Q3' as integer)) AS Q3,
                sum(CAST(quarter = 'Q4' as integer)) AS Q4
            FROM all_quarters
            GROUP BY department,job
            ORDER BY department,job;
        """
    df = pd.read_sql(query, engine)
    # only for data visualization
    # file_name =  FILE_ROUTE + f'metrics\\hired_employees_by_q.csv'
    # df.to_csv(file_name,index=False) only for data visualization

    return Response(df.to_json(orient="records"), media_type="application/json")

@app_globant.get("/hired_employees_dep")
async def hired_employees_dep():
     # check if there's data
    for table in allowed_tables:
        with engine.connect() as con:
            result = con.execute(text(f"SELECT count(1) FROM {table}")).fetchall()
            if result is None or [x[0] for x in result][0] == 0: 
                raise HTTPException(status_code=404, detail=f"There's no data in {table} or table doesn't exist")
            
     # get metrics
    query = """
            SELECT 
                h.department_id AS id,
                d.department AS department,
                COUNT(h.id) AS hired
            FROM hired_employees AS h
                JOIN departments AS d ON h.department_id=d.id
            WHERE strftime('%Y', datetime) = '2021'
            GROUP BY h.department_id,d.department
            HAVING COUNT(h.id) > (
                SELECT 
                    COUNT(h.id)/(SELECT COUNT(id) FROM departments)
                FROM hired_employees h
                WHERE strftime('%Y', h.datetime) = '2021'
            )
            ORDER BY hired DESC
            ;
        """
    df = pd.read_sql(query, engine)
    # only for data visualization
    # file_name =  FILE_ROUTE + f'metrics\\hired_employees_greater_mean.csv'
    # df.to_csv(file_name,index=False) # only for data visualization

    return Response(df.to_json(orient="records"), media_type="application/json")
    
