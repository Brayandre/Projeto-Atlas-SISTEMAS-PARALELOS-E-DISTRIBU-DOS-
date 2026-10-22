
FROM python:3.13.7-alpine3.21

WORKDIR /app

RUN pip install pyzmq grpcio-tools

COPY proto/mensagens.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. mensagens.proto

COPY broker/*.py ./
COPY python/*.py ./

CMD ["python", "-u", "servidor.py"]
