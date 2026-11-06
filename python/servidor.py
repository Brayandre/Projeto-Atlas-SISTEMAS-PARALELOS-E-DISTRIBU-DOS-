
import os
import socket

import zmq

import mensagens_pb2 as pb
from comum import (RE_CANAL, RE_USUARIO, agora_ms, fmt_ts, mostrar_publicacao,
                   mostrar_requisicao, mostrar_resposta)

NOME = os.environ.get("NOME", socket.gethostname())
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY = os.environ.get("PROXY", "tcp://proxy:5557")
DADOS = os.environ.get("DADOS", "/data")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")

ARQ_LOGINS = os.path.join(DADOS, "logins.tsv")
ARQ_CANAIS = os.path.join(DADOS, "canais.tsv")
ARQ_PUBLICACOES = os.path.join(DADOS, "publicacoes.tsv")
TAM_MAX_MENSAGEM = 500

canais = [] 

relogio_contador = 0
offset_relogio = 0
meu_rank = 0
msgs_recebidas = 0

def att_relogio(relogio_rec):
    global relogio_contador
    if relogio_rec > relogio_contador:
        relogio_contador = relogio_rec

def agora_ajst_ms():
    return agora_ms() + offset_relogio

def comunicar_referencia(operacao):
    global relogio_contador, meu_rank, offset_relogio
    relogio_contador += 1

    req = pb.ReqReferencia(
        operacao=operacao,
        nome=NOME,
        timestamp=agora_ms(),
        relogio=relogio_contador
    )
    ref_socket.send(req.SerializeToString())

    dados = ref_socket.recv()
    rep = pb.RepReferencia()
    rep.ParseFromString(dados)

    att_relogio(rep.relogio)

    if operacao == "rank":
        meu_rank = rep.rank
        print(f"{NOME}: registrado na ref com rank={meu_rank}", flush=True)

    agora_local = agora_ms()
    rtt = agora_local - req.timestamp
    hora_oficial = rep.timestamp + (rtt // 2)
    offset_relogio = hora_oficial - agora_local

def limpar(texto):
    return texto.replace("\t", " ").replace("\n", " ").replace("\r", " ")


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


def publicar(req, resp):
    global relogio_contador
    if not RE_CANAL.match(req.canal):
        resp.erro = "nome de canal invalido"
        return
    if not req.mensagem.strip():
        resp.erro = "mensagem vazia"
        return
    if len(req.mensagem) > TAM_MAX_MENSAGEM:
        resp.erro = "mensagem muito longa"
        return
    if req.canal not in canais:
        canais.append(req.canal)
        anexar(ARQ_CANAIS, req.timestamp, fmt_ts(req.timestamp), req.canal, "(via publicacao)")

    relogio_contador += 1
    pub = pb.Publicacao(
        canal=req.canal,
        usuario=req.usuario,
        mensagem=req.mensagem,
        servidor=NOME,
        timestamp=agora_ajst_ms(),
        relogio=relogio_contador
    )
    pub_socket.send_multipart([req.canal.encode(), pub.SerializeToString()])
    mostrar_publicacao(NOME, ">>> ENVIADO ", pub)
    anexar(ARQ_PUBLICACOES, pub.timestamp, fmt_ts(pub.timestamp), req.timestamp,
           pub.canal, pub.usuario, limpar(pub.mensagem))
    resp.sucesso = True


OPERACOES = {
    "login": login,
    "criar_canal": criar_canal,
    "listar_canais": listar_canais,
    "publicar": publicar,
}

pub_socket = None
ref_socket = None

def main():
    global pub_socket, ref_socket, relogio_contador, msgs_recebidas
    os.makedirs(DADOS, exist_ok=True)
    carregar_canais()

    context = zmq.Context()
    rep = context.socket(zmq.REP)
    rep.connect(BROKER)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY)

    ref_socket = context.socket(zmq.REQ)
    ref_socket.connect(REFERENCIA)

    print(f"{NOME}: server py conectado ao broker {BROKER} e  proxy {PROXY} | "
          f"dados em {DADOS} | {len(canais)} canais carregados", flush=True)

    comunicar_referencia("rank")

    while True:
        dados = rep.recv()
        req = pb.Requisicao()
        resp = pb.Resposta(servidor=NOME)
        try:
            req.ParseFromString(dados)
            att_relogio(req.relogio)
            mostrar_requisicao(NOME, "<<< RECEBIDO", req)
            resp.operacao = req.operacao
            operacao = OPERACOES.get(req.operacao)
            if operacao is None:
                resp.erro = "operacao desconhecida"
            else:
                operacao(req, resp)
            msgs_recebidas += 1
            if msgs_recebidas >= 10:
                comunicar_referencia("heartbeat")
                msgs_recebidas = 0
        except Exception as e:  
            resp.operacao = req.operacao or "desconhecida"
            resp.sucesso = False
            resp.erro = f"requisicao invalida ({e})"
            
        relogio_contador += 1
        resp.timestamp = agora_ajst_ms()
        resp.relogio = relogio_contador
        rep.send(resp.SerializeToString())
        mostrar_resposta(NOME, ">>> ENVIADO ", resp)


if __name__ == "__main__":
    main()
