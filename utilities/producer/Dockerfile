FROM python:3.11

RUN mkdir /usr/src/andean_market_data_producer
WORKDIR /usr/src/andean_market_data_producer

COPY requeriments.txt ./
RUN pip install --no-cache-dir -r requeriments.txt

COPY / .
