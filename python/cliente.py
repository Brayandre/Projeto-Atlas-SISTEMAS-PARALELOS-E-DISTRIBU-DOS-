
import os
import random
import socket
import time

import zmq

import mensagens_pb2 as pb
from comum import agora_ms, mostrar_publicacao, mostrar_requisicao, mostrar_resposta

NOME = os.environ.get("NOME", socket.gethostname())
BROKER = os.environ.get("BROKER", "tcp://broker:5555")
PROXY = os.environ.get("PROXY", "tcp://proxy:5558")

SUGESTOES_CANAIS = ["geral", "noticias", "esportes", "musica", "jogos",
                    "filmes", "tecnologia", "ciencia", "memes", "off-topic"]
FRASES = ["ola pessoal", "alguem ai?", "bom dia", "que dia corrido", "isso e um teste",
          "sistemas distribuidos e legal", "zeromq funciona bem", "mensagem aleatoria",
          "tudo certo por aqui", "ate mais"]

context = zmq.Context()
req_socket = context.socket(zmq.REQ)
req_socket.connect(BROKER)
sub_socket = context.socket(zmq.SUB)
sub_socket.connect(PROXY)

poller = zmq.Poller()
poller.register(req_socket, zmq.POLLIN)
poller.register(sub_socket, zmq.POLLIN)

conhecidos = []  
inscritos = []   


def receber_publicacoes():
    """Lê todas as publicações que já chegaram no SUB."""
    while True:
        try:
            canal, dados = sub_socket.recv_multipart(zmq.NOBLOCK)
        except zmq.Again:
            return
        recebido = agora_ms()
        if canal.decode() not in inscritos:
            continue
        pub = pb.Publicacao()
        pub.ParseFromString(dados)
        mostrar_publicacao(NOME, "<<< RECEBIDO", pub, recebido)


def requisitar(operacao, canal="", mensagem=""):
    req = pb.Requisicao(operacao=operacao, usuario=NOME, canal=canal,
                        mensagem=mensagem, timestamp=agora_ms())
    req_socket.send(req.SerializeToString())
    mostrar_requisicao(NOME, ">>> ENVIADO ", req)

    while True:
        socks = dict(poller.poll())
        if sub_socket in socks:
            receber_publicacoes()
        if req_socket in socks:
            resp = pb.Resposta()
            resp.ParseFromString(req_socket.recv())
            mostrar_resposta(NOME, "<<< RECEBIDO", resp)
            return resp


def esperar(segundos):
    """Espera o tempo pedido, mostrando as publicações que chegarem."""
    fim = time.time() + segundos
    while (resta := fim - time.time()) > 0:
        socks = dict(poller.poll(int(resta * 1000) + 1))
        if sub_socket in socks:
            receber_publicacoes()


def fazer_login():
    while not requisitar("login").sucesso:
        esperar(1)


def atualizar_canais():
    for canal in requisitar("listar_canais").canais:
        if canal not in conhecidos:
            conhecidos.append(canal)


def novo_nome_canal():
    livres = [c for c in SUGESTOES_CANAIS if c not in conhecidos]
    return random.choice(livres) if livres else f"canal-{random.randint(100, 999)}"


def inscrever(canal):
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, canal)
    inscritos.append(canal)
    print(f"{NOME}: inscrito no canal '{canal}' (inscricoes: {inscritos})", flush=True)


def main():
    print(f"{NOME}: bot Python | broker {BROKER} | proxy {PROXY}", flush=True)
    fazer_login()

    while True:
        atualizar_canais()

        if len(conhecidos) < 5:
            nome = novo_nome_canal()
            resp = requisitar("criar_canal", nome)
            if resp.sucesso or resp.erro == "canal ja existe":
                conhecidos.append(nome)

        nao_inscritos = [c for c in conhecidos if c not in inscritos]
        if len(inscritos) < 3 and nao_inscritos:
            inscrever(random.choice(nao_inscritos))

        if not conhecidos:
            esperar(1)
            continue
        canal = random.choice(conhecidos)
        for _ in range(10):
            requisitar("publicar", canal, f"{random.choice(FRASES)} ({random.randint(0, 9999)})")
            esperar(1)


if __name__ == "__main__":
    main()
