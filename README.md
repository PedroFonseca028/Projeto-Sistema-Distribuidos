# Projeto-Sistema-Distribuidos

Versão simplificada de um sistema de troca de mensagens no estilo BBS/IRC, desenvolvida para a disciplina de Sistemas Distribuídos. Os usuários são bots que fazem login, listam e criam canais e publicam mensagens em canais públicos. Os servidores se registram em um serviço de referência, mantêm relógios lógicos e físicos sincronizados, elegem um coordenador e replicam todos os dados entre si, de forma que **todos os servidores têm todos os dados**. Toda a comunicação usa ZeroMQ e todos os serviços rodam em containers orquestrados pelo Docker Compose.

## Como executar

```bash
docker compose up --build
```

Nenhuma interação é necessária: o serviço de referência, o broker e o proxy sobem, os servidores se cadastram, elegem um coordenador, sincronizam os dados entre si e os bots começam a trocar mensagens.

## Arquitetura

```
                 REQ/REP                                    PUB/SUB
 bot (REQ) ──► broker ROUTER :5555          servidor (PUB) ──► proxy XSUB :5557
               broker DEALER :5556 ◄── servidor (REP)          proxy XPUB :5558 ◄── bot (SUB, canais)
                                              │                                 ▲
                                              │ REQ/REP (relógio, eleição,      │ servidor (SUB, tópicos
                                              │          anti-entropia)         │  "servers" e "replica")
                                              ▼                                 │
                              servidor ◄──► servidor (peers :560N)  ────────────┘
                                              │
                                              │ REQ/REP (registro, lista, heartbeat)
                                              ▼
                                   referência REP :5559
```

| Serviço                | Linguagem | Função |
|------------------------|-----------|--------|
| `broker`               | Python    | Proxy ROUTER/DEALER que distribui as requisições dos bots entre os servidores (round-robin) |
| `proxy`                | Python    | Proxy XSUB/XPUB do Pub/Sub: recebe as publicações dos servidores e entrega aos inscritos |
| `reference`            | Rust      | Serviço de referência: dá o rank de cada servidor, mantém a lista dos ativos e recebe os heartbeats |
| `server1`, `server2`   | Rust      | Atendem login, canais e publicações; persistem e replicam os dados; sincronizam o relógio e elegem o coordenador |
| `bot_ana`, `bot_bruno` | Python    | Clientes automáticos (bots) |

No `docker-compose.yaml` todos os containers usam `network_mode: host`, então os serviços se encontram por `localhost` e pela porta (ex.: `tcp://localhost:5555`). Os valores padrão no código usam o nome do serviço (ex.: `tcp://broker:5555`) e podem ser trocados pelas variáveis de ambiente (ver abaixo).

### Portas

| Porta | Quem usa | Padrão |
|-------|----------|--------|
| 5555  | bots → broker (ROUTER) | REQ/REP |
| 5556  | broker → servidores (DEALER) | REQ/REP |
| 5557  | servidores → proxy (XSUB) | PUB/SUB |
| 5558  | bots e servidores → proxy (XPUB) | PUB/SUB |
| 5559  | servidores → referência | REQ/REP |
| 560N  | servidor N (ex.: `server1` = 5601, `server2` = 5602) | REQ/REP entre servidores |

## Escolhas

### Linguagens

- **Python** no cliente (bot), no broker e no proxy: código curto e direto para lógica de bot e proxies com `pyzmq`.
- **Rust** nos servidores e na referência: tipagem forte para tratar as mensagens e gerar os tipos a partir do `.proto` com `prost`.

### Serialização: Protocol Buffers

Todas as mensagens trocadas pela rede são serializadas em binário com Protocol Buffers. O arquivo [messages.proto](messages.proto) é compartilhado por todos os componentes (cópias idênticas em [server/proto/messages.proto](server/proto/messages.proto) e [reference/proto/messages.proto](reference/proto/messages.proto)).

Todos os envelopes têm `timestamp` (ms desde a época Unix) e `clock` (relógio lógico de quem enviou), e um `oneof body` com o tipo da operação. Os envelopes são:

- `Request` / `Reply`: entre bots e servidores;
- `RefRequest` / `RefReply`: entre servidores e a referência;
- `PeerRequest` / `PeerReply`: entre servidores (relógio, eleição e anti-entropia);
- `ChannelMessage`: publicação em canal (Pub/Sub, tópico = nome do canal);
- `Coordinator`: anúncio do coordenador (Pub/Sub, tópico `servers`);
- `Replica`: escrita aceita por um servidor (Pub/Sub, tópico `replica`).

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

**Servidor ↔ servidor (REQ/REP, porta 560N)**

| Operação      | Requisição                    | Resposta                          | Status possíveis |
|---------------|-------------------------------|-----------------------------------|------------------|
| Relógio       | `clock_request { name }`      | `clock_reply { status, time_ms }` | `OK`, `NAO_COORDENADOR` |
| Eleição       | `election { name, rank }`     | `election { status }`             | `OK` |
| Anti-entropia | `sync { name, known[] }`      | `sync { records[], more }`        | — |

**Servidor → proxy (Pub/Sub)**

| Tópico        | Conteúdo                                  | Quem assina |
|---------------|-------------------------------------------|-------------|
| nome do canal | `ChannelMessage` (publicação)             | bots inscritos no canal |
| `servers`     | `Coordinator { name }` (novo coordenador) | servidores |
| `replica`     | `Replica { record }` (escrita aceita)     | servidores |

Regras de validação:

- nome de usuário: 3 a 20 caracteres, só letras, números e `_`. Em caso de erro o bot repete o login;
- nome de canal: 3 a 30 caracteres, mesma regra. Canais duplicados são recusados com `CHANNEL_EXISTS`, e `servers` e `replica` são reservados (`INVALID_CHANNEL`) porque são tópicos usados entre os servidores;
- mensagem: 1 a 500 caracteres, e o canal precisa existir.

### Publicação em canais (Pub/Sub)

O fluxo de uma publicação é:

1. o bot envia `publish { channel, user, message }` ao servidor pelo broker (REQ/REP);
2. o servidor valida o pedido e publica no proxy (porta 5557, XSUB) uma mensagem ZeroMQ de duas partes: `[nome do canal, ChannelMessage serializada]`. O nome do canal é o tópico;
3. o servidor guarda a publicação e a replica para os outros servidores (ver [Replicação e consistência](#replicação-e-consistência));
4. o servidor responde ao bot com o status da publicação.

Cada bot tem **um único socket SUB** conectado ao proxy (porta 5558, XPUB) e se inscreve em vários canais nessa mesma conexão (`SUBSCRIBE` com o nome do canal). Como o filtro do ZeroMQ é por prefixo (`jogos` também casaria com `jogos2`), o bot ainda confere se o tópico recebido é exatamente um dos canais em que está inscrito. Os servidores fazem a mesma conferência nos tópicos `servers` e `replica`.

O bot usa um `zmq.Poller` sobre o socket SUB e o socket REQ. Assim, enquanto espera a resposta do servidor ou o intervalo de 1 s entre mensagens, ele exibe as publicações que chegam, sem precisar de threads (sockets ZeroMQ não são thread-safe).

### Comportamento dos bots

Depois do login, cada bot fica em loop infinito. A cada volta ele:

1. lista os canais. Se existirem menos de 5 e `BOT_CREATE=1`, cria um novo (nome sorteado entre `geral`, `tecnologia`, `jogos`, `musica` e `noticias`);
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, sorteado da lista;
3. escolhe um canal da lista e envia 10 mensagens aleatórias, com 1 s de intervalo.

As regras são reavaliadas a cada volta para que os bots cheguem a 3 inscrições e os servidores a 5 canais. Por padrão, o bot_ana cria canais (`BOT_CREATE=1`) e o bot_bruno não (`BOT_CREATE=0`).

## Serviço de referência

O serviço de referência é um processo separado, em Rust, que escuta na porta 5559. Ele **não** atende bots nem publica mensagens. Suas funções são:

- **Rank:** cada servidor se cadastra com seu nome e recebe um número de ordem (1, 2, ...). Se o servidor se cadastrar de novo com o mesmo nome enquanto ainda está na lista, recebe o mesmo rank.
- **Lista:** devolve todos os servidores ativos, ordenados pelo rank. Os servidores usam essa lista para saber quem são os outros (eleição e anti-entropia).
- **Heartbeat:** cada servidor envia um heartbeat a cada 10 mensagens de bots **ou** a cada 5 s, o que vier primeiro (o envio por tempo garante que um servidor com pouco tráfego não saia da lista). A referência responde `OK` se o servidor está cadastrado, ou `NAO_CADASTRADO` se não está (por exemplo, depois de reiniciar). Nesse caso, o servidor se cadastra de novo.
- **Remoção:** um servidor que fica sem heartbeat por mais de `HEARTBEAT_TIMEOUT` segundos (padrão 10) é removido da lista.

A referência **não devolve a hora**: o relógio físico vem do coordenador (ver abaixo).

## Relógios

Há dois tipos de relógio, e cada um tem uma finalidade:

### Relógio lógico (Lamport)

Cada componente tem um contador `clock`, que vai em todas as mensagens:

- **antes de enviar**, o contador é incrementado e enviado junto com a mensagem;
- **ao receber**, o contador fica com o **maior** valor entre o seu e o recebido, e depois é incrementado em 1.

Assim, se uma mensagem causou outra, o valor da primeira é sempre menor que o da segunda. Os bots não enviam o campo `clock` (fica em 0); os servidores e a referência mantêm o relógio lógico. Cada registro replicado guarda o `clock` da origem, usado para desempatar a ordem dos registros.

### Relógio físico (algoritmo de Berkeley)

Cada servidor mantém um **ajuste** (`offset`) sobre a hora do próprio container. A hora usada nas mensagens é `hora do sistema + offset`. O relógio do container nunca é alterado.

A cada **15 mensagens de bots**, cada servidor que **não** é o coordenador envia `clock_request` ao coordenador; a sua hora vai no `timestamp` do pedido. O coordenador aplica o algoritmo de Berkeley:

1. guarda a diferença entre a hora de quem pediu e a sua;
2. calcula a média das diferenças conhecidas (a dele entra com 0) e soma essa média ao próprio ajuste, ou seja, o coordenador também anda para a média;
3. devolve a nova hora em `clock_reply { time_ms }`.

Quem pediu adota a hora recebida mais metade do tempo de ida e volta da mensagem. Assim todos os relógios convergem para a média, e não para a hora de um servidor específico. Se o coordenador não responde (ou responde `NAO_COORDENADOR`), o servidor inicia uma eleição.

A variável `CLOCK_OFFSET_MS` permite começar um servidor com o relógio adiantado ou atrasado, para ver a sincronização acontecer.

## Eleição de coordenador (algoritmo do valentão)

Cada servidor tem um **rank**, dado pela referência. Ao iniciar, e sempre que o coordenador não responde:

1. o servidor consulta a lista da referência e envia `election { name, rank }` a todos os servidores de rank **maior** que o dele;
2. se algum responder `OK`, ele espera o anúncio do novo coordenador (se o anúncio não chegar em 5 s, faz outra eleição);
3. se ninguém responder, ele vira o coordenador e publica `Coordinator { name }` no tópico `servers` do proxy.

Quando um servidor recebe `election` de um servidor de rank menor, responde `OK` e inicia a própria eleição, porque agora é a vez dele disputar.

Todos os servidores assinam o tópico `servers` e atualizam o nome do coordenador ao receber o anúncio. Dois cuidados extras:

- o coordenador também recebe o próprio anúncio; se ele não voltar pelo proxy em 2 s (por exemplo, porque a inscrição ainda não tinha chegado ao proxy na partida), o anúncio é publicado de novo;
- se um coordenador recebe o anúncio de outro coordenador, pede nova eleição, e o maior rank vence.

## Replicação e consistência

### O problema

O broker distribui as requisições dos bots entre os servidores com round-robin. Sem replicação, cada servidor guardava só uma parte dos logins, canais e publicações: se um servidor parasse, essa parte do histórico se perdia, e quem consultasse um servidor via apenas o que estava nele. Um sintoma visível era o `CHANNEL_NOT_FOUND`: um bot listava um canal em um servidor e publicava em outro, que não conhecia o canal.

O objetivo é que **todos os servidores tenham todos os dados**.

### Método escolhido: consistência eventual com protocolo epidêmico (push + anti-entropia)

Entre os métodos vistos na aula de teoria, escolhemos **consistência eventual** com propagação das atualizações por um **protocolo epidêmico**, que combina:

- **push:** quem recebe uma escrita a propaga imediatamente para as outras réplicas;
- **pull (anti-entropia):** de tempos em tempos, uma réplica compara o que tem com o que outra tem e busca o que falta.

Todos os servidores são réplicas iguais: qualquer um aceita escritas (não há primário). A garantia é que, se as escritas pararem, depois de um tempo curto todas as réplicas ficam **idênticas**.

Esse método serve bem ao projeto porque:

- um chat tolera que uma mensagem apareça em outro servidor alguns milissegundos depois; ele não precisa de consistência estrita;
- as operações praticamente não entram em conflito: logins e publicações são eventos novos, que só precisam ser acumulados. O único conflito possível, o mesmo canal criado em dois servidores ao mesmo tempo, tem uma solução simples (ver abaixo);
- nenhuma escrita depende de outro servidor estar no ar. Se um servidor cai, os outros continuam aceitando tudo, e quando ele volta a anti-entropia entrega o que ele perdeu;
- reaproveita o que já existia: o proxy Pub/Sub (push), o REQ/REP entre servidores (pull), a lista da referência (quem são as réplicas) e os relógios de Berkeley e Lamport (ordem dos registros).

**Por que não os outros métodos?**

- **Primário-backup (escrita remota), usando o coordenador como primário:** toda escrita teria que passar pelo coordenador e esperar a sua resposta. Durante uma eleição nenhum servidor conseguiria escrever, e um coordenador que caísse no meio de uma escrita poderia deixar as réplicas diferentes.
- **Replicação ativa:** exige multicast com ordem total (um sequenciador ou acordo entre todos), o que tem a mesma dependência de um ponto central.
- **Quórum:** com 2 servidores, um quórum de escrita exige os dois no ar, então a queda de um bloquearia todas as escritas.

### Como foi implementado

**1. Cada escrita vira um registro com identificador único.** Login, criação de canal e publicação aceitos por um servidor geram um `Record` com:

- `origin`: quem aceitou a escrita, no formato `<servidor>@<hora de início da execução>` (ex.: `server1@1793580847079`);
- `seq`: número de sequência dentro dessa origem (1, 2, 3, ...);
- `timestamp`: a hora física (Berkeley) da origem quando aceitou a escrita;
- `clock`: o relógio lógico (Lamport) da origem nesse momento;
- os dados (`login`, `channel` ou `message`).

O par `(origin, seq)` identifica o registro em todo o sistema. Por isso aplicar o mesmo registro duas vezes não muda nada (a operação é idempotente): um registro recebido por push e de novo pela anti-entropia é guardado uma vez só.

**2. Push pelo tópico `replica`.** Logo depois de guardar uma escrita, o servidor publica `[replica] [Replica { record }]` no proxy. Todos os servidores assinam o tópico, guardam os registros que ainda não têm e ignoram os próprios, que voltam pelo proxy.

**3. Pull pela anti-entropia.** O Pub/Sub não garante a entrega: um servidor desligado, ou que ainda não se inscreveu, perde as mensagens. Por isso:

- **na partida**, antes de atender os bots, o servidor sincroniza com todos os outros servidores da lista da referência;
- **a cada 5 s**, sincroniza com um outro servidor, em revezamento.

A sincronização usa um **vetor de versões**: o servidor envia `sync { known[] }` dizendo, para cada origem, os intervalos de `seq` que já tem (ex.: `server1@legado: 1–481`, `server2@1793580847370: 1–6`). O outro servidor responde com todos os registros que não estão nesses intervalos, em ordem de origem e `seq`, no máximo 500 por resposta (`more: true` indica que há mais). Quem pediu guarda o lote e repete até não faltar nada. Como cada resposta só traz registros que faltam, a troca sempre avança e termina.

**4. Leitura sem duplicatas.** A lista de canais é montada a partir de todos os registros de canal, sem repetir nomes e em ordem de criação (`timestamp`, depois `clock`, depois `origin`). Como todas as réplicas têm os mesmos registros, todas devolvem a mesma lista na mesma ordem.

**5. Persistência.** Os registros (locais e replicados) vão para os mesmos arquivos de antes, agora com `origin`, `seq` e `clock` (ver [Persistência](#persistência)). Ao ligar, o servidor monta o índice `origem → seqs` a partir do disco, e assim a anti-entropia só pede o que realmente falta.

### Mudanças no método para funcionar no projeto

- **Push pelo proxy, e não direto entre servidores:** no protocolo epidêmico clássico, cada réplica envia a atualização a algumas outras. Aqui o proxy XSUB/XPUB já entrega uma mensagem a todos os inscritos, então um único `send` faz a atualização chegar a todos os servidores.
- **Parceiro da anti-entropia em revezamento, e não sorteado:** a lista de réplicas vem da referência, e o servidor percorre essa lista em ordem. Com poucos servidores, isso garante que todos são consultados regularmente.
- **Vetor de versões com intervalos:** em vez de comparar bases inteiras, quem pede descreve o que tem em poucos intervalos de `seq` por origem. Com isso, até um buraco no meio (um registro perdido no Pub/Sub) é encontrado e preenchido.
- **Uma origem por execução:** o `seq` não precisa ser salvo em disco. Cada vez que o servidor liga, ele começa uma origem nova (`nome@hora de início`) com `seq` 1, então um reinício nunca reaproveita um identificador, mesmo que o disco tenha sido perdido.
- **Conflito na criação de canal:** se dois servidores criam o mesmo canal ao mesmo tempo, os dois registros são mantidos (descartar um criaria buracos no vetor de versões). O canal aparece uma vez só na lista, e vale a criação mais antiga.
- **Dados anteriores à replicação:** os registros gravados antes desta parte não tinham `origin`/`seq`. Ao ligar, o servidor dá a eles a origem `<servidor>@legado` e um `seq` na ordem dos arquivos, e regrava os arquivos. Assim o histórico antigo de cada servidor também é replicado para os outros.

### O que se pode observar

- Ao subir, cada servidor mostra quantos registros carregou do disco e, depois da anti-entropia inicial, com quantos ficou (`REPLICA   pronto com ...`).
- Toda escrita aparece como `PUBLICADO [replica] ...` no servidor que a aceitou e como `RECEBIDO  [replica] ...` / `REPLICA   registro ... guardado` nos outros.
- Derrubando um servidor (`docker compose stop server2`) e ligando de novo, ele busca o que perdeu: `REPLICA   anti-entropia com server1: 41 registros recebidos, 41 novos`.
- Depois que os bots param, os arquivos de `data/server1` e `data/server2` têm os mesmos registros (mesmos `origin` e `seq`), mesmo que em ordem diferente de chegada.

Às vezes a anti-entropia traz 1 ou 2 registros "novos" com tudo funcionando. Isso acontece quando ela consulta o outro servidor logo depois de uma escrita, antes do push chegar pelo proxy. Quando o push chega depois, ele é descartado pelo `(origin, seq)`.

## Exibição das mensagens

Bots e servidores exibem o conteúdo completo de toda mensagem enviada ou recebida:

```
bot_ana    | [22:03:04] ENVIADO   timestamp: 1793052184950 publish { channel: "geral" user: "bot_ana" message: "acabei de terminar o trabalho" }
server1    | RECEBIDO  Request { timestamp: 1793052184950, clock: 0, body: Some(Publish(Publish { channel: "geral", ... })) }
server1    | PUBLICADO [geral] ChannelMessage { timestamp: 1793052184951, channel: "geral", user: "bot_ana", ... }
server1    | PUBLICADO [replica] Replica { timestamp: 1793052184952, clock: 41, record: Some(Record { origin: "server1@1793052100000", seq: 12, ... }) }
server1    | ENVIADO   Reply { timestamp: 1793052184953, clock: 42, body: Some(Publish(PublishReply { status: "OK" })) }
server2    | RECEBIDO  [replica] Replica { timestamp: 1793052184952, clock: 41, record: Some(Record { origin: "server1@1793052100000", seq: 12, ... }) }
server2    | REPLICA   registro server1@1793052100000#12 guardado
bot_ana    | [22:03:04] PUB       #geral <bot_ana> acabei de terminar o trabalho (enviado 22:03:04.951, recebido 22:03:04.952)
bot_ana    | [22:03:04] RECEBIDO  timestamp: 1793052184953 publish { status: "OK" }
```

As publicações recebidas pelos bots mostram o canal, o autor, a mensagem, o horário de envio (timestamp da `ChannelMessage`) e o horário de recebimento no bot.

A exceção é a resposta da anti-entropia, que pode ter centenas de registros: ela aparece resumida (`Sync(500 registros, more: true)`).

Os servidores também registram os eventos de cada parte, com um prefixo: `ELEIÇÃO` (eleição e coordenador), `BERKELEY` (cálculo no coordenador), `relógio sincronizado com ...`, `heartbeat OK` e `REPLICA` (migração, anti-entropia e registros guardados).

## Variáveis de ambiente

| Componente | Variável | Padrão | Função |
|------------|----------|--------|--------|
| bot | `BOT_NAME` | `bot_<aleatório>` | nome do bot |
| bot | `BROKER_URL` | `tcp://broker:5555` | endereço do broker |
| bot | `PROXY_URL` | `tcp://proxy:5558` | endereço do proxy (inscrições) |
| bot | `BOT_CREATE` | `1` | `1` cria canais, `0` não cria |
| servidor | `SERVER_ID` | `1` | número do servidor (nome `server<N>`) |
| servidor | `BROKER_URL` | `tcp://broker:5556` | endereço do broker (DEALER) |
| servidor | `PROXY_URL` | `tcp://proxy:5557` | endereço do proxy para publicar (canais, anúncios e réplicas) |
| servidor | `PROXY_SUB_URL` | `tcp://localhost:5558` | endereço do proxy para assinar `servers` e `replica` |
| servidor | `REF_URL` | `tcp://reference:5559` | endereço da referência |
| servidor | `PEER_HOST` | `localhost` | host dos outros servidores |
| servidor | `PEER_PORT_BASE` | `5600` | o servidor N escuta os outros servidores na porta `PEER_PORT_BASE + N` |
| servidor | `CLOCK_OFFSET_MS` | `0` | diferença inicial do relógio físico (para testar o Berkeley) |
| referência | `REF_URL` | `tcp://*:5559` | endereço em que a referência escuta |
| referência | `HEARTBEAT_TIMEOUT` | `10` | segundos sem heartbeat até remover um servidor |

O endereço de outro servidor é derivado do nome que ele usa na referência: `server<N>` → `tcp://PEER_HOST:(PEER_PORT_BASE + N)`.

## Persistência

Cada servidor tem seus próprios arquivos em `data/server<N>/` (montado como volume em `/app/data`). Os arquivos não são compartilhados entre servidores: cada um tem a sua cópia completa, mantida pela replicação.

- `server_<N>.json`: logins (`user`, `timestamp`) e canais criados (`name`, `created_by`, `timestamp`), cada um com `origin`, `seq` e `clock`. O arquivo é gravado em um `.tmp` e depois renomeado, para não corromper se o container cair durante a escrita.
- `server_<N>_messages.jsonl`: publicações, uma linha JSON por mensagem com `channel`, `user`, `message`, `request_timestamp` (envio do pedido pelo bot), `timestamp` (publicação no canal), `origin`, `seq` e `clock`. O formato é *append-only*: cada publicação, local ou replicada, acrescenta uma linha sem reescrever o arquivo. Os lotes recebidos pela anti-entropia são gravados de uma vez.

Os registros ficam na ordem em que chegaram a cada servidor, que pode variar entre servidores. A ordem do histórico é dada por `timestamp` (Berkeley), depois `clock` (Lamport), depois `origin`; como todos os servidores têm os mesmos registros, todos chegam à mesma ordem.

Se um arquivo não puder ser lido, o servidor guarda uma cópia em `<arquivo>.invalido` antes de continuar. No `.jsonl`, só as linhas ilegíveis são descartadas (por exemplo, uma linha cortada porque o container caiu durante a gravação) e o arquivo é regravado; no `.json`, o servidor começa com logins e canais vazios. Nos dois casos, o que se perdeu localmente volta pela anti-entropia, se outro servidor tiver.

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

- **Consistência eventual, não estrita.** Logo depois de uma escrita existe uma janela de milissegundos em que outro servidor ainda não a recebeu. Um bot que cria um canal em um servidor e publica nele em outro, exatamente nessa janela, recebe `CHANNEL_NOT_FOUND`. Antes da replicação isso era comum; agora é raro.
- **Cada servidor faz uma coisa de cada vez.** Enquanto espera a resposta de outro servidor ou da referência, ele continua atendendo os outros servidores, mas não os bots; com timeouts de 1 s (servidores) e 3 s (referência), um bot pode ter que esperar ou repetir o pedido.
- **A eleição também roda na inicialização.** Se um servidor inicia antes dos outros, pode virar coordenador sozinho e depois ceder a vez. O sistema converge, mas o log mostra anúncios repetidos.
- **Os bots não enviam o relógio lógico** (`clock` fica em 0 nas requisições dos bots).
- **Os nomes dos servidores precisam seguir o padrão `server<N>`**, porque a porta usada entre os servidores é derivada do nome.
- **Todo o histórico fica em memória** em cada servidor, para responder rápido à anti-entropia. É adequado ao volume do projeto, mas não a um histórico muito grande.
