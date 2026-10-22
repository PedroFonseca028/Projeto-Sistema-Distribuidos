import os
import random
import time
from datetime import datetime

import zmq

import messages_pb2 as pb 

BOT_NAME = os.environ.get("BOT_NAME", "bot_" + str(random.randint(1000, 9999)))
BROKER_URL = os.environ.get("BROKER_URL", "tcp://broker:5555")
BOT_CREATE = os.environ.get("BOT_CREATE", "1") == "1"

CHANNEL_POOL = ["geral", "tecnologia", "jogos", "musica", "noticias"]

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.setsockopt(zmq.RCVTIMEO, 3000)  # não trava para sempre se o broker não responder
socket.setsockopt(zmq.LINGER, 0)
socket.connect(BROKER_URL)


def agora_ms():
    return int(time.time() * 1000)


def hora():
    return datetime.now().strftime("%H:%M:%S")


def mostrar(direcao, msg):
    texto = " ".join(str(msg).split())  # str() da mensagem em uma linha só
    print(f"[{hora()}] {direcao:<9} {texto}", flush=True)


def enviar(req):
    req.timestamp = agora_ms()
    mostrar("ENVIADO", req)
    socket.send(req.SerializeToString())
    try:
        resp = pb.Reply()
        resp.ParseFromString(socket.recv())
    except zmq.Again:
        print(f"[{hora()}] ERRO      timeout sem resposta", flush=True)
        return None
    mostrar("RECEBIDO", resp)
    return resp


# 1. Login: repete até o servidor aceitar
while True:
    req = pb.Request()
    req.login.user = BOT_NAME
    resp = enviar(req)
    if resp and resp.login.status == "OK":
        break
    time.sleep(2)

# 2. Listar canais
req = pb.Request()
req.list_channels.SetInParent()
resp = enviar(req)

canais = []
if resp:
    canais = list(resp.channels.channels)

# 3. Criar canal, se configurado e se a lista estiver pequena
if BOT_CREATE and len(canais) < 3:
    disponiveis = [c for c in CHANNEL_POOL if c not in canais]
    if disponiveis:
        req = pb.Request()
        req.create_channel.name = random.choice(disponiveis)
        req.create_channel.user = BOT_NAME
        enviar(req)

# 4. Listar de novo para ver o resultado
req = pb.Request()
req.list_channels.SetInParent()
enviar(req)