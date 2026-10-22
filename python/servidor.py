
import os
import socket

import zmq

import mensagens_pb2 as pb
from comum import RE_CANAL, RE_USUARIO, agora_ms, fmt_ts, mostrar_requisicao, mostrar_resposta

NOME = os.environ.get("NOME", socket.gethostname())
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
DADOS = os.environ.get("DADOS", "/data")

ARQ_LOGINS = os.path.join(DADOS, "logins.tsv")
ARQ_CANAIS = os.path.join(DADOS, "canais.tsv")

canais = []

def anexar(arquivo, *colunas):
    with open(arquivo, "a", encoding="utf-8") as f:
        f.write("\t".join(str(c) for c in colunas) + "\n")


def carregar_canais():
    if not os.path.exists(ARQ_CANAIS):
        return
    with open(ARQ_CANAIS, encoding="utf-8") as f:
        for linha in f:
            partes = linha.rstrip("\n").split("\t")
            if len(partes) >= 3 and RE_CANAL.match(partes[2]) and partes[2] not in canais:
                canais.append(partes[2])


def login(req, resp):
    if not RE_USUARIO.match(req.usuario):
        resp.erro = "nome de usuario invalido"
        return
    anexar(ARQ_LOGINS, req.timestamp, fmt_ts(req.timestamp), req.usuario)
    resp.sucesso = True


def criar_canal(req, resp):
    if not RE_CANAL.match(req.canal):
        resp.erro = "nome de canal invalido"
    elif req.canal in canais:
        resp.erro = "canal ja existe"
    else:
        canais.append(req.canal)
        anexar(ARQ_CANAIS, req.timestamp, fmt_ts(req.timestamp), req.canal, req.usuario)
        resp.sucesso = True


def listar_canais(req, resp):
    resp.canais.extend(canais)
    resp.sucesso = True


OPERACOES = {
    "login": login,
    "criar_canal": criar_canal,
    "listar_canais": listar_canais,
}


def main():
    os.makedirs(DADOS, exist_ok=True)
    carregar_canais()

    context = zmq.Context()
    rep = context.socket(zmq.REP)
    rep.connect(BROKER)
    print(f"{NOME}: servidor Python conectado ao broker {BROKER} | dados em {DADOS} | "
          f"{len(canais)} canais carregados", flush=True)

    while True:
        dados = rep.recv()
        req = pb.Requisicao()
        resp = pb.Resposta(servidor=NOME)
        try:
            req.ParseFromString(dados)
            mostrar_requisicao(NOME, "<<< RECEBIDO", req)
            resp.operacao = req.operacao
            operacao = OPERACOES.get(req.operacao)
            if operacao is None:
                resp.erro = "operacao desconhecida"
            else:
                operacao(req, resp)
        except Exception as e:
            resp.operacao = req.operacao or "desconhecida"
            resp.sucesso = False
            resp.erro = f"requisicao invalida ({e})"

        resp.timestamp = agora_ms()
        rep.send(resp.SerializeToString())
        mostrar_resposta(NOME, ">>> ENVIADO ", resp)


if __name__ == "__main__":
    main()
