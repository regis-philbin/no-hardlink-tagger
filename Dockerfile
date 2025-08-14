# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Upgrade pip and install dependencies
RUN pip install --upgrade pip \
    && pip install python-qbittorrent

# Copy the cleanup script into the container
COPY qbit_cleanup.py .

# Ensure Python output is unbuffered so logs appear immediately
ENV PYTHONUNBUFFERED=1

# Command to run the cleanup script
CMD ["python", "qbit_cleanup.py"]
