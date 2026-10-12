FROM apache/spark:4.2.0

USER root
WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["python3", "/app/src/stream_processor.py"]