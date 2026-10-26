# Projeto-Sistema-Distribuidos

Versão simplificada de um sistema de troca de mensagens no estilo BBS/IRC, desenvolvida para a disciplina de Sistemas Distribuídos. Os usuários são bots que fazem login, listam e criam canais e publicam mensagens em canais públicos. Toda a comunicação usa ZeroMQ e todos os serviços rodam em containers orquestrados pelo Docker Compose.

## Como executar

```bash
docker compose up --build
```

Nenhuma interação é necessária: os bots começam a trocar mensagens assim que os containers sobem.

## Arquitetura

```
                  REQ/REP                                    PUB/SUB
 bot (REQ) ──► broker ROUTER :5555          servidor (PUB) ──► proxy XSUB :5557
               broker DEALER :5556 ◄── servidor (REP)          proxy XPUB :5558 ◄── bot (SUB)
```

| Serviço             | Linguagem | Função |
|---------------------|-----------|--------|
| `broker`            | Python    | Proxy ROUTER/DEALER que distribui as requisições dos bots entre os servidores |
| `proxy`             | Python    | Proxy XSUB/XPUB do Pub/Sub: recebe as publicações dos servidores e entrega aos bots inscritos |
| `server1`, `server2`| Rust      | Atendem login, listagem/criação de canais e publicação; persistem os dados em disco |
| `bot_ana`, `bot_bruno` | Python | Clientes automáticos (bots) |

Os serviços se comunicam pela rede criada pelo Docker Compose, usando o nome do serviço como endereço (ex.: `tcp://broker:5555`, `tcp://proxy:5558`).

## Escolhas

### Linguagens

- **Python** no cliente (bot), no broker e no proxy: código curto e direto para lógica de bot e proxies com `pyzmq`.
- **Rust** no servidor: tipagem forte para tratar as mensagens e gerar os tipos a partir do `.proto` com `prost`.

### Serialização: Protocol Buffers

Todas as mensagens são serializadas em binário com Protocol Buffers. O arquivo [messages.proto](messages.proto) é compartilhado pelas duas linguagens (cópia idêntica em [server/proto/messages.proto](server/proto/messages.proto)):

- `Request` / `Reply` são os envelopes do REQ/REP. Os dois têm o campo `timestamp` (ms desde a época Unix) e um `oneof body` com o tipo da operação (`login`, `list_channels`, `create_channel`, `publish`).
- `ChannelMessage` é a mensagem publicada nos canais: `timestamp` do envio da publicação, `channel`, `user` e `message`.

### Mensagens trocadas

| Operação         | Requisição (bot → servidor)     | Resposta (servidor → bot) | Status possíveis |
|------------------|---------------------------------|---------------------------|------------------|
| Login            | `login { user }`                | `login { status }`        | `OK`, `INVALID_USERNAME` |
| Listar canais    | `list_channels {}`              | `channels { status, channels[] }` | `OK` |
| Criar canal      | `create_channel { name, user }` | `create_channel { status }` | `OK`, `INVALID_CHANNEL`, `CHANNEL_EXISTS` |
| Publicar         | `publish { channel, user, message }` | `publish { status }` | `OK`, `CHANNEL_NOT_FOUND`, `INVALID_MESSAGE`, `PUBLISH_ERROR` |

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

1. lista os canais. Se existirem menos de 5, cria um novo (nome sorteado entre `geral`, `tecnologia`, `jogos`, `musica` e `noticias`);
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, sorteado da lista;
3. escolhe um canal da lista e envia 10 mensagens aleatórias, com 1 s de intervalo.

As regras são reavaliadas a cada volta para que os bots cheguem a 3 inscrições e os servidores a 5 canais.

### Exibição das mensagens

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

### Persistência

Cada servidor tem seus próprios arquivos em `data/server<N>/` (montado como volume em `/app/data`). Os arquivos não são compartilhados entre servidores.

- `server_<N>.json`: logins (usuário + timestamp) e canais criados (nome, criador, timestamp). O arquivo é gravado em um `.tmp` e depois renomeado, para não corromper se o container cair durante a escrita.
- `server_<N>_messages.jsonl`: publicações, uma linha JSON por mensagem com `channel`, `user`, `message`, `request_timestamp` (envio do pedido pelo bot) e `timestamp` (publicação no canal). O formato é *append-only*: cada publicação acrescenta uma linha sem reescrever o arquivo, o que mantém a gravação rápida mesmo com muitas mensagens e permite recuperar todo o histórico lendo o arquivo linha a linha.

### Limitação conhecida

Os servidores ainda não sincronizam dados entre si, e o broker distribui as requisições entre eles. Por isso um bot pode listar os canais em um servidor e publicar em outro que ainda não conhece aquele canal, recebendo `CHANNEL_NOT_FOUND`. Como os bots criam canais até cada servidor ter 5 (sorteados da mesma lista de 5 nomes), os servidores convergem para os mesmos canais e esses erros deixam de acontecer depois de algumas rodadas.
