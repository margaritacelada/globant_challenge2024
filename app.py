from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text


FILE_ROUTE = 'C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\'

#con = sqlite3.connect("globant.db")
#cur = con.cursor()

engine = create_engine('sqlite:///C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\globant.db', echo=False)

department_schema = {'id':'int32','department':'str'}
hired_employees_schema = {'id':'int32','name':'str','datetime':'str','department_id':'int32','job_id':'int32'}
jobs_schema = {'id':'int32','job':'str'}
allowed_tables = ["departments", "jobs", "hired_employees"]

app_globant = FastAPI()

@app_globant.post("/batch_load")
async def batch_load(table_name:str):

    nrows_insert = 1000
    file_name =  FILE_ROUTE + f'{table_name}.csv'
    
    if table_name not in allowed_tables:
        return HTTPException(status_code=404, detail=f"This table {table_name} is not available")

    if table_name == 'departments':
        table_schema = department_schema
    if table_name == 'jobs':
        table_schema = jobs_schema
    if table_name == 'hired_employees':
        table_schema = hired_employees_schema

    #df = pd.read_csv( f'C:\\Users\\ASUS\\Desktop\\Globant\\globant_challenge2024\\historical_data\\{file}.csv',sep=',', header=0)
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
    # df.reset_index(inplace=True)

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