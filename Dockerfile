# Base image
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Work directory /app
WORKDIR /globant_challenge2024

# Copy requirements.txt file in the container 
COPY requirements.txt .

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all work directory content into container in /globant_challenge2024
COPY . .

# execute application
# CMD ["python", "tu_script.py"]
