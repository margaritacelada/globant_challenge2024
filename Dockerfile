# Base image
FROM python:3.9

# Work directory /app
WORKDIR /app

# Copy requirements.txt file in the container 
COPY requirements.txt .

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all work directory content into container in /app
COPY . .

# execute applicationEjecuta tu aplicación
# CMD ["python", "tu_script.py"]
