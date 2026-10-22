
import os
import random
import socket
import time

import zmq

import mensagens_pb2 as pb
from comum import agora_ms, mostrar_requisicao, mostrar_resposta

NOME = os.environ.get("NOME", socket.gethostname())
BROKER = os.environ.get("BROKER", "tcp://broker:5555")

SUGESTOES_CANAIS = ["geral", "noticias", "esportes", "musica", "jogos",
                    "filmes", "tecnologia", "ciencia", "memes", "off-topic"]

context = zmq.Context()
req_socket = context.socket(zmq.REQ)
req_socket.connect(BROKER)


def requisitar(operacao, canal=""):
    req = pb.Requisicao(operacao=operacao, usuario=NOME, canal=canal, timestamp=agora_ms())
    req_socket.send(req.SerializeToString())
    mostrar_requisicao(NOME, ">>> ENVIADO ", req)

    resp = pb.Resposta()
    resp.ParseFromString(req_socket.recv())
    mostrar_resposta(NOME, "<<< RECEBIDO", resp)
    return resp


def fazer_login():
    while not requisitar("login").sucesso:
        time.sleep(1)


def main():
    print(f"{NOME}: bot Python conectando ao broker {BROKER}", flush=True)
    fazer_login()

    while True:
        canais = list(requisitar("listar_canais").canais)
        if len(canais) < 5:
            requisitar("criar_canal", random.choice(SUGESTOES_CANAIS))

        time.sleep(random.uniform(2, 4))


if __name__ == "__main__":
    main()
