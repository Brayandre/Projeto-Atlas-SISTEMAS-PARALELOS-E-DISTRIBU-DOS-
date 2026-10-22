CC = gcc
CFLAGS = -O2 -Wall -Wextra
LIBS = -lzmq -lprotobuf-c

all: servidor cliente

mensagens.pb-c.c mensagens.pb-c.h: mensagens.proto
	protoc-c --c_out=. mensagens.proto

servidor: servidor.c comum.c mensagens.pb-c.c mensagens.pb-c.h comum.h
	$(CC) $(CFLAGS) -o $@ servidor.c comum.c mensagens.pb-c.c $(LIBS)

cliente: cliente.c comum.c mensagens.pb-c.c mensagens.pb-c.h comum.h
	$(CC) $(CFLAGS) -o $@ cliente.c comum.c mensagens.pb-c.c $(LIBS)

clean:
	rm -f servidor cliente mensagens.pb-c.c mensagens.pb-c.h
