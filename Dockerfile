FROM python:3.9-slim
WORKDIR /app
RUN pip install --upgrade pip && pip install python-qbittorrent requests
COPY qbit_cleanup.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "qbit_cleanup.py"]
