# Use a lightweight Python Linux image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy dependencies and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline script
COPY pipeline.py .

# Create necessary folders inside the container
RUN mkdir hospital_inbox anonymized_data

# Run the pipeline when the container starts
CMD ["python", "-u", "pipeline.py"]