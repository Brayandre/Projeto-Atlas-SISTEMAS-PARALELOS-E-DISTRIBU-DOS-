# Bot (cliente) em Python: faz login, lista os canais e cria canais novos
# enviando requisições ao servidor através do broker (REQ).
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
    # Em caso de erro o login é repetido até funcionar
    while not requisitar("login").sucesso:
        time.sleep(1)


def main():
    print(f"{NOME}: bot Python conectando ao broker {BROKER}", flush=True)
    fazer_login()

    while True:
        canais = list(requisitar("listar_canais").canais)

        # Critério para criar canal: existem menos de 5 canais.
        # O nome é sorteado de uma lista de sugestões; às vezes o canal já
        # existe e o servidor responde com erro (tratado sem interromper o bot).
        if len(canais) < 5:
            requisitar("criar_canal", random.choice(SUGESTOES_CANAIS))

        time.sleep(random.uniform(2, 4))


if __name__ == "__main__":
    main()
