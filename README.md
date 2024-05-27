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
`curl -X POST -H "Content-Type: application/json" 'http://127.0.0.1:8000/batch_load?table_name=departments'`

como ejecutar la API:
inicializar
uvicorn app_test:app --reload

ejecutar
curl -X POST -H "Content-Type: application/json" 'http://127.0.0.1:8000/items?item=orange'

curl -X GET http://127.0.0.1:8000/items/0

curl -X POST -H "Content-Type: application/json" -d '{"title":"apple"}' 'http://127.0.0.1:8000/items

curl -X POST -H "Content-Type: application/json" 'http://127.0.0.1:8000/load_table?file=departments'
uvicorn app:app --reload