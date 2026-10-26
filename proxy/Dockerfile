FROM python:3.13-slim

WORKDIR /app

RUN pip install pyzmq

COPY proxy.py ./

CMD ["python", "proxy.py"]