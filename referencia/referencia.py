import os
import time
import zmq
import mensagens_pb2 as pb

PORTA = os.environ.get("PORTA_REF", "5559")
TIMEOUT_HEARTBEAT = int(os.environ.get("TIMEOUT_HEARTBEAT", "30"))

relogio_contador = 0
proximo_rank = 1

servidores = {}


def agora_ms():
    return int(time.time() * 1000)


def att_relogio(relogio_rec):
    global relogio_contador
    if relogio_rec > relogio_contador:
        relogio_contador = relogio_rec


def limpar_inativos():
    agora = time.time()
    inativos = [
        nome for nome, dados in servidores.items()
        if (agora - dados["ultimo_hb"]) > TIMEOUT_HEARTBEAT
    ]
    for nome in inativos:
        print(f"referencia: servidor '{nome}' (rank {servidores[nome]['rank']}) removido por inatividade", flush=True)
        del servidores[nome]


def main():
    global relogio_contador, proximo_rank

    context = zmq.Context()
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{PORTA}")

    print(f"referencia: escutando servidores em tcp://*:{PORTA}", flush=True)

    while True:
        dados = rep_socket.recv()
        req = pb.ReqReferencia()
        req.ParseFromString(dados)
        att_relogio(req.relogio)
        limpar_inativos()

        rep = pb.RepReferencia()
        rep.operacao = req.operacao

        if req.operacao == "rank":
            if req.nome not in servidores:
                servidores[req.nome] = {
                    "rank": proximo_rank,
                    "ultimo_hb": time.time()
                }
                proximo_rank += 1
            else:
                servidores[req.nome]["ultimo_hb"] = time.time()

            rep.rank = servidores[req.nome]["rank"]
            rep.status = "OK"
            print(f"referencia: [clock={relogio_contador}] RANK pedido por '{req.nome}' -> rank={rep.rank}", flush=True)

        elif req.operacao == "list":
            for nome, info in servidores.items():
                item = rep.servidores.add()
                item.nome = nome
                item.rank = info["rank"]
            rep.status = "OK"
            print(f"referencia: [clock={relogio_contador}] LIST pedido -> {len(servidores)} ativos", flush=True)

        elif req.operacao in ("heartbeat", "clock"):
            if req.nome not in servidores:
                servidores[req.nome] = {
                    "rank": proximo_rank,
                    "ultimo_hb": time.time()
                }
                proximo_rank += 1
            else:
                servidores[req.nome]["ultimo_hb"] = time.time()

            rep.rank = servidores[req.nome]["rank"]
            rep.status = "OK"
            print(f"referencia: [clock={relogio_contador}] HEARTBEAT de '{req.nome}' -> OK", flush=True)

        else:
            rep.status = f"ERRO: operacao desconhecida ({req.operacao})"

        relogio_contador += 1
        rep.relogio = relogio_contador

        rep_socket.send(rep.SerializeToString())


if __name__ == "__main__":
    main()