CC = gcc
CFLAGS = -Wall -Wextra -O2
LDLIBS = -lzmq -lprotobuf-c -lsqlite3

all: servidor cliente

mensagens.pb-c.c mensagens.pb-c.h: mensagens.proto
	protoc-c --c_out=. mensagens.proto

servidor: servidor.c comum.c mensagens.pb-c.c mensagens.pb-c.h
	$(CC) $(CFLAGS) -o servidor servidor.c comum.c mensagens.pb-c.c $(LDLIBS)

cliente: cliente.c comum.c mensagens.pb-c.c mensagens.pb-c.h
	$(CC) $(CFLAGS) -o cliente cliente.c comum.c mensagens.pb-c.c $(LDLIBS)

clean:
	rm -f servidor cliente mensagens.pb-c.c mensagens.pb-c.h