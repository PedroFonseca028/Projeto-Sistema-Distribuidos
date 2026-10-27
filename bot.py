import os
import random
import time
from datetime import datetime

import zmq

import messages_pb2 as pb

BOT_NAME = os.environ.get("BOT_NAME", "bot_" + str(random.randint(1000, 9999)))
BROKER_URL = os.environ.get("BROKER_URL", "tcp://broker:5555")
PROXY_URL = os.environ.get("PROXY_URL", "tcp://proxy:5558")

CHANNEL_POOL = ["geral", "tecnologia", "jogos", "musica", "noticias"]

FRASES = [
    "bom dia, pessoal!",
    "alguém viu as notícias de hoje?",
    "testando o canal",
    "zeromq é muito prático",
    "quem vai no evento amanhã?",
    "acabei de terminar o trabalho",
    "recomendações de música?",
    "o servidor está respondendo rápido",
]

context = zmq.Context()


def novo_socket():
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(BROKER_URL)
    return sock


socket = novo_socket()

# Uma única conexão com o proxy, inscrita em vários canais (tópicos)
sub = context.socket(zmq.SUB)
sub.connect(PROXY_URL)
inscritos = set()


def agora_ms():
    return int(time.time() * 1000)


def hora():
    return datetime.now().strftime("%H:%M:%S")


def formatar_ms(ms):
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S.%f")[:-3]


def mostrar(direcao, msg):
    texto = " ".join(str(msg).split())
    print(f"[{hora()}] {direcao:<9} {texto}", flush=True)


def receber_publicacao():
    topico, dados = sub.recv_multipart()
    # O filtro do ZeroMQ é por prefixo ("jogos" também casaria "jogos2"), então confere o nome exato
    if topico.decode() not in inscritos:
        return
    msg = pb.ChannelMessage()
    msg.ParseFromString(dados)
    recebido = agora_ms()
    print(
        f"[{hora()}] PUB       #{msg.channel} <{msg.user}> {msg.message} "
        f"(enviado {formatar_ms(msg.timestamp)}, recebido {formatar_ms(recebido)})",
        flush=True,
    )


def aguardar(segundos, req=None):
    """Exibe as publicações que chegarem durante `segundos`.
    Se `req` for informado, retorna a resposta dele assim que chegar (None no timeout)."""
    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)
    if req is not None:
        poller.register(req, zmq.POLLIN)
    limite = time.time() + segundos
    while (resto := limite - time.time()) > 0:
        eventos = dict(poller.poll(resto * 1000))
        if sub in eventos:
            receber_publicacao()
        if req is not None and req in eventos:
            return req.recv()
    return None


def enviar(req):
    global socket
    req.timestamp = agora_ms()
    mostrar("ENVIADO", req)
    socket.send(req.SerializeToString())
    dados = aguardar(3, socket)
    if dados is None:
        print(f"[{hora()}] ERRO      timeout sem resposta", flush=True)
        socket.close()
        socket = novo_socket()
        return None
    resp = pb.Reply()
    resp.ParseFromString(dados)
    mostrar("RECEBIDO", resp)
    return resp


def listar_canais():
    req = pb.Request()
    req.list_channels.SetInParent()
    resp = enviar(req)
    return list(resp.channels.channels) if resp else None


# 1. Login: repete até o servidor aceitar
while True:
    req = pb.Request()
    req.login.user = BOT_NAME
    resp = enviar(req)
    if resp and resp.login.status == "OK":
        break
    time.sleep(2)

# 2. Loop infinito: aplica as regras e publica 10 mensagens em um canal
while True:
    canais = listar_canais()
    if canais is None:
        continue

    # Menos de 5 canais: cria um novo
    if len(canais) < 5:
        disponiveis = [c for c in CHANNEL_POOL if c not in canais]
        if disponiveis:
            req = pb.Request()
            req.create_channel.name = random.choice(disponiveis)
            req.create_channel.user = BOT_NAME
            resp = enviar(req)
            if resp and resp.create_channel.status == "OK":
                canais.append(req.create_channel.name)

    # Inscrito em menos de 3 canais: inscreve em mais um
    if len(inscritos) < 3:
        opcoes = [c for c in canais if c not in inscritos]
        if opcoes:
            canal = random.choice(opcoes)
            sub.setsockopt_string(zmq.SUBSCRIBE, canal)
            inscritos.add(canal)
            print(f"[{hora()}] INSCRITO  #{canal} (canais: {', '.join(sorted(inscritos))})", flush=True)

    if not canais:
        aguardar(1)
        continue

    canal = random.choice(canais)
    for _ in range(10):
        req = pb.Request()
        req.publish.channel = canal
        req.publish.user = BOT_NAME
        req.publish.message = random.choice(FRASES)
        enviar(req)
        aguardar(1)
