# globant_challenge2024

como ejecutar la API:
inicializar
uvicorn app_test:app --reload

ejecutar
curl -X POST -H "Content-Type: application/json" 'http://127.0.0.1:8000/items?item=orange'

curl -X GET http://127.0.0.1:8000/items/0

curl -X POST -H "Content-Type: application/json" -d '{"title":"apple"}' 'http://127.0.0.1:8000/items

curl -X POST -H "Content-Type: application/json" 'http://127.0.0.1:8000/load_table?file=departments'
uvicorn app:app --reload