FROM python:3.12-slim

WORKDIR /app

COPY main.py /app/
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

RUN pip install --no-cache-dir pymongo
RUN pip install --no-cache-dir requests

RUN mkdir -p /app/csv_files /app/logs
VOLUME /app/csv_files
VOLUME /app/logs

ENV MONGO_USER=admin
ENV MONGO_PASS=admin123
ENV MONGO_DB=mydb
ENV MONGO_COLLECTION=mycollection
ENV MONGO_HOST=mongo
ENV BATCH_SIZE=10000
ENV CHECK_INTERVAL=10

ENTRYPOINT ["/app/entrypoint.sh"]
