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
