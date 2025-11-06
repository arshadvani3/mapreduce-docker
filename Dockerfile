# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install RPyC (Remote Python Call library)
RUN pip install --no-cache-dir rpyc

# Copy application files
COPY coordinator.py /app/
COPY worker.py /app/

# Create directory for downloaded datasets
RUN mkdir -p /app/txt

# Expose port 18861 for RPyC workers
EXPOSE 18861

# Default command (will be overridden in docker-compose)
CMD ["python", "worker.py"]
