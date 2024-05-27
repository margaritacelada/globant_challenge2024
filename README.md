# globant_challenge2024
### Public repository dedicated to Coding Challenge requested by Globant data engineer role

#### Intend


#### Repository content

#### Useful commands

- Create database and tables in sqlite3: 
`python db.py`

- Initialize API and auto update for changes: 
`uvicorn app:app_globant --reload`

- Call post method to ingest historical data: 
`curl -X POST -H "Content-Type: application/json" -d '{"file_name":"departments","table_name":"departments","nrows":"1000"}' 'http://127.0.0.1:8000/batch_load'` 

- Call post method to create a backup table in avro format
`curl -X POST -H "Content-Type: application/json" -d '{"table_name":"departments"}' 'http://127.0.0.1:8000/backup_table'` 

- Call post method to restore table
`curl -X POST -H "Content-Type: application/json" -d '{"table_name":"departments"}' 'http://127.0.0.1:8000/restore_table'` 

- Call get method to get metrics hired employees by quarter
`curl -X GET http://127.0.0.1:8000/hired_employees_q` 

#### Deployment (Draft)
Repository includes a basic Dockerfile whit basic commands trying to show the content
`docker build -t globant_challenge .` 
