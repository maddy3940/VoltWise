# Use an official Python runtime as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the prophet_predict script to the working directory
COPY prophet_predict.py .

# Run the prophet_predict script when the container starts
CMD ["python", "prophet_predict.py"]
