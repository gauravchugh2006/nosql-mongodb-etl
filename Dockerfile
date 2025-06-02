# Use an official lightweight Python image.
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy the requirements file and install dependencies.
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code.
COPY main.py .
# COPY medical_data.csv .

# Copy the data folder (this ensures all CSV files inside data/ are accessible)
COPY data /app/data

# Run the migration script when the container starts.
CMD ["python", "main.py"]