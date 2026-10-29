# Projeto-Sistema-Distribuidos

Versão simplificada de um sistema de troca de mensagens no estilo BBS/IRC, desenvolvida para a disciplina de Sistemas Distribuídos. Os usuários são bots que fazem login, listam e criam canais e publicam mensagens em canais públicos. Os servidores se registram em um serviço de referência, mantêm relógios lógicos e físicos sincronizados e elegem um coordenador. Toda a comunicação usa ZeroMQ e todos os serviços rodam em containers orquestrados pelo Docker Compose.

## Como executar

```bash
docker compose up --build
```

Nenhuma interação é necessária: o serviço de referência, o broker e o proxy sobem primeiro, os servidores se cadastram, elegem um coordenador e os bots começam a trocar mensagens.

## Arquitetura

```
                 REQ/REP                                    PUB/SUB
 bot (REQ) ──► broker ROUTER :5555          servidor (PUB) ──► proxy XSUB :5557
               broker DEALER :5556 ◄── servidor (REP)          proxy XPUB :5558 ◄── bot (SUB)
                                              │                                 ▲
                                              │ REQ/REP (relógio, eleição)      │ SUB "servers"
                                              ▼                                 │
                              servidor ◄──► servidor (peers :600N)  ────────────┘
                                              │
                                              │ REQ/REP (registro, lista, heartbeat)
                                              ▼
                                   referência REP :5559
```

| Serviço                | Linguagem | Função |
|------------------------|-----------|--------|
| `broker`               | Python    | Proxy ROUTER/DEALER que distribui as requisições dos bots entre os servidores |
| `proxy`                | Python    | Proxy XSUB/XPUB do Pub/Sub: recebe as publicações dos servidores e entrega aos inscritos |
| `reference`            | Rust      | Serviço de referência: dá o rank de cada servidor, mantém a lista dos ativos e recebe os heartbeats |
| `server1`, `server2`   | Rust      | Atendem login, canais e publicações; persistem os dados; sincronizam o relógio e elegem o coordenador |
| `bot_ana`, `bot_bruno` | Python    | Clientes automáticos (bots) |

Os serviços se comunicam pela rede criada pelo Docker Compose, usando o nome do serviço como endereço (ex.: `tcp://broker:5555`, `tcp://proxy:5558`, `tcp://reference:5559`).

### Portas

| Porta | Quem usa | Padrão |
|-------|----------|--------|
| 5555  | bots → broker (ROUTER) | REQ/REP |
| 5556  | broker → servidores (DEALER) | REQ/REP |
| 5557  | servidores → proxy (XSUB) | PUB/SUB |
| 5558  | bots e servidores → proxy (XPUB) | PUB/SUB |
| 5559  | servidores → referência | REQ/REP |
| 600N  | servidor N (ex.: `server1` = 6001) | REQ/REP entre servidores |

## Escolhas

### Linguagens

- **Python** no cliente (bot), no broker e no proxy: código curto e direto para lógica de bot e proxies com `pyzmq`.
- **Rust** nos servidores e na referência: tipagem forte para tratar as mensagens e gerar os tipos a partir do `.proto` com `prost`.

### Serialização: Protocol Buffers

Todas as mensagens trocadas pela rede são serializadas em binário com Protocol Buffers. O arquivo [messages.proto](messages.proto) é compartilhado por todos os componentes (cópias idênticas em [server/proto/messages.proto](server/proto/messages.proto) e [reference/proto/messages.proto](reference/proto/messages.proto)).

Todos os envelopes têm `timestamp` (ms desde a época Unix) e `clock` (relógio lógico de quem enviou), e um `oneof body` com o tipo da operação. Os envelopes são:

- `Request` / `Reply`: entre bots e servidores;
- `RefRequest` / `RefReply`: entre servidores e a referência;
- `PeerRequest` / `PeerReply`: entre servidores (relógio e eleição);
- `ChannelMessage`: publicação em canal (Pub/Sub);
- `Coordinator`: anúncio do coordenador (Pub/Sub, tópico `servers`).

O JSON só é usado para persistência em disco, nunca nas mensagens da rede.

### Mensagens trocadas

**Bot → servidor (REQ/REP, via broker)**

| Operação         | Requisição                      | Resposta                  | Status possíveis |
|------------------|---------------------------------|---------------------------|------------------|
| Login            | `login { user }`                | `login { status }`        | `OK`, `INVALID_USERNAME` |
| Listar canais    | `list_channels {}`              | `channels { status, channels[] }` | `OK` |
| Criar canal      | `create_channel { name, user }` | `create_channel { status }` | `OK`, `INVALID_CHANNEL`, `CHANNEL_EXISTS` |
| Publicar         | `publish { channel, user, message }` | `publish { status }` | `OK`, `CHANNEL_NOT_FOUND`, `INVALID_MESSAGE`, `PUBLISH_ERROR` |

**Servidor → referência (REQ/REP)**

| Operação   | Requisição               | Resposta                       | Status possíveis |
|------------|--------------------------|--------------------------------|------------------|
| Registro   | `register { name }`      | `register { rank }`            | — |
| Lista      | `list {}`                | `list { servers[name, rank] }` | — |
| Heartbeat  | `heartbeat { name }`     | `heartbeat { status }`         | `OK`, `NAO_CADASTRADO` |

**Servidor ↔ servidor (REQ/REP, porta 600N)**

| Operação   | Requisição                    | Resposta                       | Status possíveis |
|------------|-------------------------------|--------------------------------|------------------|
| Relógio    | `clock_request { name }`      | `clock_reply { status, time_ms }` | `OK`, `NAO_COORDENADOR` |
| Eleição    | `election { name, rank }`     | `election { status }`          | `OK` |

**Servidor → proxy (Pub/Sub)**

| Tópico     | Conteúdo                         | Quem assina |
|------------|----------------------------------|-------------|
| nome do canal | `ChannelMessage` (publicação) | bots inscritos no canal |
| `servers`  | `Coordinator { name }` (novo coordenador) | servidores |

Regras de validação:

- nome de usuário: 3 a 20 caracteres, só letras, números e `_`. Em caso de erro o bot repete o login;
- nome de canal: 3 a 30 caracteres, mesma regra. Canais duplicados são recusados com `CHANNEL_EXISTS`;
- mensagem: 1 a 500 caracteres, e o canal precisa existir no servidor que recebeu o pedido.

### Publicação em canais (Pub/Sub)

O fluxo de uma publicação é:

1. o bot envia `publish { channel, user, message }` ao servidor pelo broker (REQ/REP);
2. o servidor valida o pedido e publica no proxy (porta 5557, XSUB) uma mensagem ZeroMQ de duas partes: `[nome do canal, ChannelMessage serializada]`. O nome do canal é o tópico;
3. o servidor responde ao bot com o status da publicação.

Cada bot tem **um único socket SUB** conectado ao proxy (porta 5558, XPUB) e se inscreve em vários canais nessa mesma conexão (`SUBSCRIBE` com o nome do canal). Como o filtro do ZeroMQ é por prefixo (`jogos` também casaria com `jogos2`), o bot ainda confere se o tópico recebido é exatamente um dos canais em que está inscrito.

O bot usa um `zmq.Poller` sobre o socket SUB e o socket REQ. Assim, enquanto espera a resposta do servidor ou o intervalo de 1 s entre mensagens, ele exibe as publicações que chegam, sem precisar de threads (sockets ZeroMQ não são thread-safe).

### Comportamento dos bots

Depois do login, cada bot fica em loop infinito. A cada volta ele:

1. lista os canais. Se existirem menos de 5 e `BOT_CREATE=1`, cria um novo (nome sorteado entre `geral`, `tecnologia`, `jogos`, `musica` e `noticias`);
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, sorteado da lista;
3. escolhe um canal da lista e envia 10 mensagens aleatórias, com 1 s de intervalo.

As regras são reavaliadas a cada volta para que os bots cheguem a 3 inscrições e os servidores a 5 canais. Por padrão, o bot_ana cria canais (`BOT_CREATE=1`) e o bot_bruno não (`BOT_CREATE=0`).

## Serviço de referência

O serviço de referência é um processo separado, em Rust, que escuta na porta 5559. Ele **não** atende bots nem publica mensagens. Suas funções são:

- **Rank:** cada servidor se cadastra com seu nome e recebe um número de ordem (1, 2, ...). Se o servidor se cadastrar de novo com o mesmo nome, recebe o mesmo rank.
- **Lista:** devolve todos os servidores ativos, ordenados pelo rank. Os servidores usam essa lista para saber quem são os outros.
- **Heartbeat:** cada servidor envia um heartbeat a cada 10 mensagens de bots. A referência responde `OK` se o servidor está cadastrado, ou `NAO_CADASTRADO` se não está (por exemplo, depois de reiniciar). Nesse caso, o servidor se cadastra de novo.
- **Remoção:** um servidor que fica sem heartbeat por mais de `HEARTBEAT_TIMEOUT` segundos (padrão 10) é removido da lista.

A referência **não devolve a hora**: o relógio físico vem do coordenador (ver abaixo).

## Relógios

Há dois tipos de relógio, e cada um tem uma finalidade:

### Relógio lógico (Lamport)

Cada componente tem um contador `clock`, que vai em todas as mensagens:

- **antes de enviar**, o contador é incrementado e enviado junto com a mensagem;
- **ao receber**, o contador fica com o **maior** valor entre o seu e o recebido, e depois é incrementado em 1.

Assim, se uma mensagem causou outra, o valor da primeira é sempre menor que o da segunda. Os bots ainda não enviam o campo `clock` (ficam com 0); só os servidores e a referência mantêm o relógio lógico.

### Relógio físico (algoritmo de Berkeley)

Cada servidor mantém um **ajuste** (`offset`) sobre a hora do próprio container. A hora usada nas mensagens é `hora do sistema + offset`. O relógio do container nunca é alterado.

A cada **15 mensagens de bots**, um servidor atualiza seu relógio:

1. se ele é o **coordenador**, calcula o ajuste com o algoritmo de Berkeley: pergunta a hora de todos os outros servidores, calcula a média das diferenças (contando a dele, que vale 0) e soma essa média ao próprio ajuste;
2. se ele **não** é o coordenador, pede a hora correta ao coordenador (`clock_request`) e ajusta o próprio `offset` para que a hora fique igual à do coordenador;
3. se o coordenador não responde, inicia uma eleição (ver abaixo).

Os servidores que não são coordenadores pedem a hora ao coordenador, e o coordenador devolve a hora que ele mesmo ajustou no último cálculo de Berkeley.

## Eleição de coordenador (algoritmo do valentão)

Cada servidor tem um **rank**, dado pela referência. Quando um servidor precisa de um coordenador e não tem um, ou o coordenador não responde:

1. ele consulta a lista da referência e envia `election { name, rank }` a todos os servidores de rank **maior** que o dele;
2. se algum responder `OK`, ele para: o servidor maior vai assumir a eleição;
3. se ninguém responder, ele vira o coordenador e publica `Coordinator { name }` no tópico `servers` do proxy.

Quando um servidor recebe `election` de um servidor de rank menor, responde `OK` e inicia a própria eleição, porque agora é a vez dele disputar.

Todos os servidores assinam o tópico `servers` e atualizam o nome do coordenador ao receber o anúncio.

## Exibição das mensagens

Bots e servidores exibem o conteúdo completo de toda mensagem enviada ou recebida:

```
bot_ana    | [22:03:04] ENVIADO   timestamp: 1793052184950 publish { channel: "geral" user: "bot_ana" message: "acabei de terminar o trabalho" }
server1    | RECEBIDO  Request { timestamp: 1793052184950, body: Some(Publish(Publish { channel: "geral", ... })) }
server1    | PUBLICADO [geral] ChannelMessage { timestamp: 1793052184951, channel: "geral", user: "bot_ana", ... }
server1    | ENVIADO   Reply { timestamp: 1793052184953, body: Some(Publish(PublishReply { status: "OK" })) }
bot_ana    | [22:03:04] PUB       #geral <bot_ana> acabei de terminar o trabalho (enviado 22:03:04.951, recebido 22:03:04.952)
bot_ana    | [22:03:04] RECEBIDO  timestamp: 1793052184953 publish { status: "OK" }
```

As publicações recebidas mostram o canal, o autor, a mensagem, o horário de envio (timestamp da `ChannelMessage`) e o horário de recebimento no bot.

Os servidores também registram os eventos de sincronização, por exemplo `heartbeat OK`, `eleição iniciada por ...`, `anunciado no tópico servers: coordenador é ...`, `Berkeley: média ...` e `relógio sincronizado com o coordenador ...`.

## Variáveis de ambiente

| Componente | Variável | Padrão | Função |
|------------|----------|--------|--------|
| bot | `BOT_NAME` | `bot_<aleatório>` | nome do bot |
| bot | `BROKER_URL` | `tcp://broker:5555` | endereço do broker |
| bot | `PROXY_URL` | `tcp://proxy:5558` | endereço do proxy (inscrições) |
| bot | `BOT_CREATE` | `1` | `1` cria canais, `0` não cria |
| servidor | `SERVER_ID` | `1` | número do servidor (nome `server<N>`) |
| servidor | `BROKER_URL` | `tcp://broker:5556` | endereço do broker (DEALER) |
| servidor | `PROXY_URL` | `tcp://proxy:5557` | endereço do proxy (publicações e anúncios) |
| servidor | `REF_URL` | `tcp://reference:5559` | endereço da referência |
| referência | `REF_URL` | `tcp://*:5559` | endereço em que a referência escuta |
| referência | `HEARTBEAT_TIMEOUT` | `10` | segundos sem heartbeat até remover um servidor |

Quando `REF_URL` contém `localhost`, os servidores usam `localhost` como endereço dos outros servidores (é o caso dos testes com `network_mode: host`). Caso contrário, usam o nome do serviço (ex.: `server2`).

## Persistência

Cada servidor tem seus próprios arquivos em `data/server<N>/` (montado como volume em `/app/data`). Os arquivos não são compartilhados entre servidores.

- `server_<N>.json`: logins (usuário + timestamp) e canais criados (nome, criador, timestamp). O arquivo é gravado em um `.tmp` e depois renomeado, para não corromper se o container cair durante a escrita.
- `server_<N>_messages.jsonl`: publicações, uma linha JSON por mensagem com `channel`, `user`, `message`, `request_timestamp` (envio do pedido pelo bot) e `timestamp` (publicação no canal). O formato é *append-only*: cada publicação acrescenta uma linha sem reescrever o arquivo, o que mantém a gravação rápida mesmo com muitas mensagens e permite recuperar todo o histórico lendo o arquivo linha a linha.

## Estrutura do repositório

```
bot.py                      bot (Python)
broker/broker.py            broker ROUTER/DEALER (Python)
proxy/proxy.py              proxy XSUB/XPUB (Python)
reference/src/main.rs       serviço de referência (Rust)
server/src/main.rs          servidor (Rust)
messages.proto              contrato Protocol Buffers
docker-compose.yaml         orquestração dos containers
data/                       arquivos persistidos pelos servidores
```

## Limitações conhecidas

- **Canais não são sincronizados entre servidores.** O broker distribui as requisições entre eles, então um bot pode listar um canal em um servidor e publicar em outro que ainda não o conhece, recebendo `CHANNEL_NOT_FOUND`. Como os bots criam canais até cada servidor ter 5 (sorteados da mesma lista de 5 nomes), os servidores convergem para os mesmos canais e esses erros deixam de acontecer depois de algumas rodadas.
- **Cada servidor atende uma coisa de cada vez.** Enquanto um servidor espera a resposta de outro (por exemplo, durante a sincronização), ele não responde a pedidos de eleição ou de relógio. Isso pode fazer um servidor parecer indisponível e disparar uma eleição desnecessária.
- **A eleição também roda na inicialização.** Se um servidor inicia antes dos outros, pode virar coordenador sozinho e depois ceder a vez. O sistema converge, mas o log mostra anúncios repetidos.
- **Os bots não enviam o relógio lógico** (`clock` fica em 0 nas requisições dos bots).
- **Os servidores precisam conseguir alcançar uns aos outros** pelo endereço derivado do nome (`server<N>` na porta `600N`), o que exige que os nomes dos serviços no Compose sigam esse padrão.