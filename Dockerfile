FROM python:3.9-slim

WORKDIR /app

RUN pip install python-qbittorrent

COPY qbit_cleanup.py .
COPY config.ini .

CMD ["python", "qbit_cleanup.py"]
