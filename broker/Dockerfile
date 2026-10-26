FROM python:3.13-slim

WORKDIR /app

RUN pip install pyzmq

COPY broker.py ./

CMD ["python", "broker.py"]