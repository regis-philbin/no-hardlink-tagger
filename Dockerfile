FROM python:3.9-slim

WORKDIR /app

# Upgrade pip and install the latest python-qbittorrent
RUN pip install --upgrade pip
RUN pip install --upgrade python-qbittorrent

COPY qbit_cleanup.py .

CMD ["python", "qbit_cleanup.py"]
