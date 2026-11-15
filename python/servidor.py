import os
import socket
import sqlite3
import zmq

import mensagens_pb2 as pb
from comum import (RE_CANAL, RE_USUARIO, agora_ms, fmt_ts, mostrar_publicacao,
                   mostrar_requisicao, mostrar_resposta)

NOME = os.environ.get("NOME", socket.gethostname())
DADOS = os.environ.get("DADOS", "/data")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY = os.environ.get("PROXY", "tcp://proxy:5557")
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy:5558")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")
PORTASERVER = int(os.environ.get("PORTASERVER", "5560"))

ARQ_DB = os.path.join(DADOS, "atlas.db")
ARQ_LOGINS = os.path.join(DADOS, "logins.tsv")
ARQ_CANAIS = os.path.join(DADOS, "canais.tsv")
ARQ_PUBLICACOES = os.path.join(DADOS, "publicacoes.tsv")
TAM_MAX_MENSAGEM = 500

canais = []

relogio_contador = 0
offset_relogio = 0
meu_rank = 0
msgs_recebidas = 0
coordenador = ""

context = None
por_rep = None
pub_socket = None
sub_socket = None
ref_socket = None
db_conn = None


def att_relogio(relogio_rec):
    global relogio_contador
    if relogio_rec > relogio_contador:
        relogio_contador = relogio_rec


def agora_ajst_ms():
    return agora_ms() + offset_relogio


def limpar(texto):
    return texto.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def anexar(arquivo, *colunas):
    with open(arquivo, "a", encoding="utf-8") as f:
        f.write("\t".join(str(c) for c in colunas) + "\n")


# --- BANCO DE DADOS SQLITE E PERSISTÊNCIA ---

def iniciar_db():
    global db_conn
    db_conn = sqlite3.connect(ARQ_DB)
    cur = db_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            data_hora TEXT,
            usuario TEXT,
            origem TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS canais (
            nome TEXT PRIMARY KEY,
            timestamp INTEGER,
            data_hora TEXT,
            criador TEXT,
            origem TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS publicacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            data_hora TEXT,
            ts_requisicao INTEGER,
            canal TEXT,
            usuario TEXT,
            mensagem TEXT,
            servidor TEXT
        )
    """)
    db_conn.commit()


def carregar_canais():
    cur = db_conn.cursor()
    cur.execute("SELECT nome FROM canais")
    for (nome_canal,) in cur.fetchall():
        if RE_CANAL.match(nome_canal) and nome_canal not in canais:
            canais.append(nome_canal)

    if os.path.exists(ARQ_CANAIS):
        with open(ARQ_CANAIS, encoding="utf-8") as f:
            for linha in f:
                partes = linha.rstrip("\n").split("\t")
                if len(partes) >= 3 and RE_CANAL.match(partes[2]) and partes[2] not in canais:
                    canais.append(partes[2])


def gravar_login_local(ts, usuario, origem):
    data_str = fmt_ts(ts)
    db_conn.execute(
        "INSERT INTO logins (timestamp, data_hora, usuario, origem) VALUES (?, ?, ?, ?)",
        (ts, data_str, usuario, origem)
    )
    db_conn.commit()
    anexar(ARQ_LOGINS, ts, data_str, usuario)


def gravar_canal_local(ts, canal, criador, origem):
    if canal not in canais:
        canais.append(canal)
    data_str = fmt_ts(ts)
    db_conn.execute(
        "INSERT OR IGNORE INTO canais (nome, timestamp, data_hora, criador, origem) VALUES (?, ?, ?, ?, ?)",
        (canal, ts, data_str, criador, origem)
    )
    db_conn.commit()
    anexar(ARQ_CANAIS, ts, data_str, canal, criador)


def gravar_publicacao_local(ts_pub, ts_req, canal, usuario, mensagem, origem):
    data_str = fmt_ts(ts_pub)
    msg_limpa = limpar(mensagem)
    db_conn.execute(
        """INSERT INTO publicacoes
           (timestamp, data_hora, ts_requisicao, canal, usuario, mensagem, servidor)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (ts_pub, data_str, ts_req, canal, usuario, msg_limpa, origem)
    )
    db_conn.commit()
    anexar(ARQ_PUBLICACOES, ts_pub, data_str, ts_req, canal, usuario, msg_limpa)


def enviar_replica(tipo, usuario="", canal="", mensagem="", timestamp=0, ts_requisicao=0):
    """Envia os dados gravados para todos os outros servidores via tópico 'replica'."""
    global relogio_contador
    relogio_contador += 1
    rep_msg = pb.Replica(
        tipo=tipo,
        servidor=NOME,
        usuario=usuario,
        canal=canal,
        mensagem=mensagem,
        timestamp=timestamp,
        ts_requisicao=ts_requisicao,
        relogio=relogio_contador
    )
    pub_socket.send_multipart([b"replica", rep_msg.SerializeToString()])



def comunicar_referencia(operacao):
    global relogio_contador, meu_rank
    relogio_contador += 1

    req = pb.ReqReferencia(
        operacao=operacao,
        nome=NOME,
        timestamp=agora_ajst_ms(),
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

    return rep


def anuncia_coord():
    global coordenador, relogio_contador
    coordenador = NOME
    relogio_contador += 1
    pub = pb.Publicacao(
        canal="servers",
        usuario=NOME,
        mensagem=NOME,
        servidor=NOME,
        timestamp=agora_ajst_ms(),
        relogio=relogio_contador
    )
    pub_socket.send_multipart([b"servers", pub.SerializeToString()])
    print(f"{NOME}: novo coordenador (rank={meu_rank})", flush=True)


def requisitar_server(alvo_nome, operacao):
    global relogio_contador
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 1500)
    sock.setsockopt(zmq.SNDTIMEO, 1500)
    sock.setsockopt(zmq.LINGER, 0)
    endereco = f"tcp://{alvo_nome}" if ":" in alvo_nome else f"tcp://{alvo_nome}:{PORTASERVER}"
    sock.connect(endereco)

    relogio_contador += 1
    req = pb.ReqServidor(
        operacao=operacao,
        remetente=NOME,
        rank=meu_rank,
        timestamp=agora_ajst_ms(),
        relogio=relogio_contador
    )

    try:
        sock.send(req.SerializeToString())
        dados = sock.recv()
        rep = pb.RepServidor()
        rep.ParseFromString(dados)
        att_relogio(rep.relogio)
        sock.close()
        return req.timestamp, rep
    except zmq.ZMQError:
        sock.close()
        return req.timestamp, None


def iniciar_eleicao():
    rep_lista = comunicar_referencia("list")

    alguem_acima_respondeu = False
    for srv in rep_lista.servidores:
        if srv.nome == NOME:
            continue
        if srv.rank < meu_rank:
            _, rep = requisitar_server(srv.nome, "eleicao")
            if rep is not None and rep.status == "OK":
                alguem_acima_respondeu = True

    if not alguem_acima_respondeu:
        anuncia_coord()


def sincronizar_relogio():
    global offset_relogio, coordenador
    if not coordenador:
        iniciar_eleicao()

    if coordenador == NOME:
        return

    t_envio, rep = requisitar_server(coordenador, "relogio")
    if rep is None:
        print(f"{NOME}: coordenador '{coordenador}' indisponivel | iniciando nova eleicao", flush=True)
        iniciar_eleicao()
        return

    agora_local = agora_ms()
    rtt = agora_local - t_envio
    hora_coord = rep.timestamp + (rtt // 2)
    offset_relogio = hora_coord - agora_local
    print(f"{NOME}: relogio sincronizado com '{coordenador}' (offset={offset_relogio}ms)", flush=True)


def tratarpor_req():
    global relogio_contador
    dados = por_rep.recv()
    req = pb.ReqServidor()
    req.ParseFromString(dados)
    att_relogio(req.relogio)

    relogio_contador += 1
    rep = pb.RepServidor(
        operacao=req.operacao,
        status="OK",
        timestamp=agora_ajst_ms(),
        relogio=relogio_contador
    )
    por_rep.send(rep.SerializeToString())

    if req.operacao == "eleicao":
        print(f"{NOME}: recebi pedido de ELEICAO de '{req.remetente}' (rank={req.rank}) -> respondi OK", flush=True)
        if meu_rank < req.rank and coordenador != NOME:
            iniciar_eleicao()
    elif req.operacao == "relogio":
        print(f"{NOME}: forneci hora para '{req.remetente}'", flush=True)


def tratar_subservers():
    """Trata tanto anúncios de coordenador ('servers') quanto replicação de dados ('replica')."""
    global coordenador
    while True:
        try:
            topico_bytes, dados = sub_socket.recv_multipart(zmq.NOBLOCK)
        except zmq.Again:
            return

        topico = topico_bytes.decode("utf-8", errors="ignore")

        if topico == "servers":
            pub = pb.Publicacao()
            pub.ParseFromString(dados)
            att_relogio(pub.relogio)
            coordenador = pub.mensagem or pub.servidor
            print(f"{NOME}: novo coordenador eleito: '{coordenador}'", flush=True)

        elif topico == "replica":
            rep_msg = pb.Replica()
            rep_msg.ParseFromString(dados)
            att_relogio(rep_msg.relogio)
            if rep_msg.servidor == NOME:
                continue

            if rep_msg.tipo == "login":
                gravar_login_local(rep_msg.timestamp, rep_msg.usuario, rep_msg.servidor)
            elif rep_msg.tipo == "canal":
                gravar_canal_local(rep_msg.timestamp, rep_msg.canal, rep_msg.usuario, rep_msg.servidor)
            elif rep_msg.tipo == "publicacao":
                if rep_msg.canal not in canais:
                    gravar_canal_local(rep_msg.ts_requisicao, rep_msg.canal, "(via replica)", rep_msg.servidor)
                gravar_publicacao_local(
                    rep_msg.timestamp, rep_msg.ts_requisicao,
                    rep_msg.canal, rep_msg.usuario, rep_msg.mensagem, rep_msg.servidor
                )
            print(f"{NOME}: [REPLICA] {rep_msg.tipo} de '{rep_msg.servidor}' gravado no SQLite", flush=True)


#  OPERAÇÕES

def login(req, resp):
    if not RE_USUARIO.match(req.usuario):
        resp.erro = "nome de usuario invalido"
        return
    gravar_login_local(req.timestamp, req.usuario, NOME)
    enviar_replica("login", usuario=req.usuario, timestamp=req.timestamp)
    resp.sucesso = True


def criar_canal(req, resp):
    if not RE_CANAL.match(req.canal):
        resp.erro = "nome de canal invalido"
    elif req.canal in canais:
        resp.erro = "canal ja existe"
    else:
        gravar_canal_local(req.timestamp, req.canal, req.usuario, NOME)
        enviar_replica("canal", usuario=req.usuario, canal=req.canal, timestamp=req.timestamp)
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
        gravar_canal_local(req.timestamp, req.canal, "(via publicacao)", NOME)
        enviar_replica("canal", usuario="(via publicacao)", canal=req.canal, timestamp=req.timestamp)

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

    gravar_publicacao_local(pub.timestamp, req.timestamp, pub.canal, pub.usuario, pub.mensagem, NOME)
    enviar_replica(
        "publicacao",
        usuario=pub.usuario,
        canal=pub.canal,
        mensagem=pub.mensagem,
        timestamp=pub.timestamp,
        ts_requisicao=req.timestamp
    )
    resp.sucesso = True


OPERACOES = {
    "login": login,
    "criar_canal": criar_canal,
    "listar_canais": listar_canais,
    "publicar": publicar,
}


def main():
    global pub_socket, ref_socket, relogio_contador, msgs_recebidas, context, por_rep, sub_socket
    os.makedirs(DADOS, exist_ok=True)
    iniciar_db()
    carregar_canais()

    context = zmq.Context()
    rep = context.socket(zmq.REP)
    rep.connect(BROKER)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY)

    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(PROXY_SUB)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "replica")

    ref_socket = context.socket(zmq.REQ)
    ref_socket.connect(REFERENCIA)

    por_rep = context.socket(zmq.REP)
    por_rep.bind(f"tcp://*:{PORTASERVER}")

    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(por_rep, zmq.POLLIN)
    poller.register(sub_socket, zmq.POLLIN)

    print(f"{NOME}: server py conectado | SQLite em {ARQ_DB} | {len(canais)} canais carregados", flush=True)

    comunicar_referencia("rank")

    while True:
        socks = dict(poller.poll())

        if por_rep in socks:
            tratarpor_req()
        if sub_socket in socks:
            tratar_subservers()
        if rep in socks:
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

                if msgs_recebidas % 10 == 0:
                    comunicar_referencia("heartbeat")

                if msgs_recebidas % 15 == 0:
                    sincronizar_relogio()

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