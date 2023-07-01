# Use the prebuilt image from DockerHub
FROM maddy39/prophet_base:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the prophet_predict script to the working directory
COPY prophet_predict.py .

# Run the prophet_predict script when the container starts
CMD ["python", "prophet_predict.py"]
