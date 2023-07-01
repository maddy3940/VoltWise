# Use an official Python runtime as the base image
FROM python:3.9-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to the working directory
# COPY requirements.txt .

# # Set MAX_CONCURRENT_WORKERS environment variable
# ENV MAX_CONCURRENT_WORKERS=2

# Install the dependencies
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir fastparquet==2023.4.0
RUN pip install --no-cache-dir numpy==1.25.0
RUN pip install --no-cache-dir pandas==2.0.3
RUN pip install --no-cache-dir prophet==1.1.4
RUN pip install --no-cache-dir python-dateutil==2.8.2
RUN pip install --no-cache-dir pytz==2023.3
RUN pip install --no-cache-dir s3fs
RUN pip install --no-cache-dir pyarrow==12.0.1

# Copy the prophet_train script to the working directory
COPY prophet_train.py .

# Run the prophet_train script when the container starts
CMD ["python", "prophet_train.py"]
