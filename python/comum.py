
import re
import time
from datetime import datetime

RE_USUARIO = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
RE_CANAL = re.compile(r"^[a-z0-9_-]{1,32}$")


def agora_ms():
    return int(time.time() * 1000)


def fmt_ts(ms):
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _linha(quem, direcao, tipo, campos):
    corpo = ", ".join(f"{k}: {v}" for k, v in campos)
    print(f"[{fmt_ts(agora_ms())}] {quem:<14} {direcao} {tipo} {{{corpo}}}", flush=True)


def mostrar_requisicao(quem, direcao, r):
    campos = [("operacao", r.operacao), ("usuario", r.usuario)]
    if r.canal:
        campos.append(("canal", r.canal))
    if r.mensagem:
        campos.append(("mensagem", f'"{r.mensagem}"'))
    campos.append(("timestamp", fmt_ts(r.timestamp)))
    _linha(quem, direcao, "REQ", campos)


def mostrar_resposta(quem, direcao, r):
    campos = [("operacao", r.operacao), ("sucesso", "true" if r.sucesso else "false")]
    if r.erro:
        campos.append(("erro", r.erro))
    if r.operacao == "listar_canais":
        campos.append(("canais", "[" + ", ".join(r.canais) + "]"))
    campos.append(("servidor", r.servidor))
    campos.append(("timestamp", fmt_ts(r.timestamp)))
    _linha(quem, direcao, "REP", campos)


def mostrar_publicacao(quem, direcao, p, recebido_ms=None):
    campos = [("canal", p.canal), ("usuario", p.usuario), ("mensagem", f'"{p.mensagem}"'),
              ("servidor", p.servidor), ("enviado", fmt_ts(p.timestamp))]
    if recebido_ms is not None:
        campos.append(("recebido", fmt_ts(recebido_ms)))
    _linha(quem, direcao, "PUB", campos)
