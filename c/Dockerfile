# Imagem do cliente/servidor em C (ZeroMQ + protobuf-c)
FROM alpine:3.21

RUN apk add --no-cache build-base zeromq-dev protobuf-c-dev protobuf-c-compiler

WORKDIR /app

COPY proto/mensagens.proto .
COPY c/*.c c/*.h c/Makefile ./
RUN make

CMD ["./servidor"]
