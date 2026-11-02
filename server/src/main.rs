use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/chat.rs"));
}

use proto::{
    peer_reply, peer_request, record, ref_reply, ref_request, reply, request, ChannelList,
    ChannelMessage, ChannelRecord, ClockReply, ClockRequest, Coordinator, CreateChannelReply,
    Election, ElectionReply, Heartbeat, Known, ListServers, LoginRecord, LoginReply, MessageRecord,
    PeerReply, PeerRequest, PublishReply, Record, RefReply, RefRequest, Register, Replica, Reply,
    Request, SeqRange, SyncReply, SyncRequest,
};

// Tópico do Pub/Sub em que o servidor eleito anuncia que é o coordenador.
const TOPICO_SERVERS: &str = "servers";
// Tópico do Pub/Sub em que cada servidor publica as escritas que aceitou (réplica por push).
const TOPICO_REPLICA: &str = "replica";
// Heartbeat a cada 10 mensagens de clientes e sincronização do relógio a cada 15.
const MSGS_HEARTBEAT: u32 = 10;
const MSGS_SYNC: u32 = 15;
// Com pouco tráfego, manda heartbeat por tempo para não sair da lista da referência
// (a eleição depende da lista e dos ranks da referência).
const INTERVALO_HEARTBEAT: Duration = Duration::from_secs(5);
// Anti-entropia: a cada 5 s pede a outro servidor os registros que ainda não tem.
const INTERVALO_ANTI_ENTROPIA: Duration = Duration::from_secs(5);
// Máximo de registros por resposta de sincronização e de rodadas seguidas com o mesmo servidor.
const LIMITE_SYNC: usize = 500;
const MAX_RODADAS_SYNC: usize = 50;
// Tempo máximo esperando a resposta da referência e de outro servidor.
const TIMEOUT_REF: Duration = Duration::from_secs(3);
const TIMEOUT_PEER: Duration = Duration::from_secs(1);
// Depois de receber OK na eleição, espera este tempo pelo anúncio do novo coordenador.
const ESPERA_ANUNCIO: Duration = Duration::from_secs(5);
// Se o próprio anúncio não voltar pelo proxy neste tempo, o coordenador publica de novo.
const REENVIO_ANUNCIO: Duration = Duration::from_secs(2);
// Por quanto tempo o coordenador considera a diferença de relógio informada por um servidor.
const VALIDADE_BERKELEY: Duration = Duration::from_secs(60);

// Dados persistidos em data/server_<id>.json (um arquivo por servidor).
#[derive(Serialize, Deserialize, Default)]
struct Store {
    logins: Vec<LoginRec>,
    channels: Vec<ChannelRec>,
}

// Todo registro tem origin + seq (identificador único da escrita) e o relógio lógico da
// origem. Os campos têm valor padrão para ler os arquivos gravados antes da replicação.
#[derive(Serialize, Deserialize)]
struct LoginRec {
    user: String,
    timestamp: u64,
    #[serde(default)]
    origin: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    clock: u64,
}

#[derive(Serialize, Deserialize)]
struct ChannelRec {
    name: String,
    created_by: String,
    timestamp: u64,
    #[serde(default)]
    origin: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    clock: u64,
}

// Publicações ficam em data/server_<id>_messages.jsonl (uma linha JSON por publicação).
#[derive(Serialize, Deserialize)]
struct MessageRec {
    channel: String,
    user: String,
    message: String,
    request_timestamp: u64, // quando o bot enviou o pedido
    timestamp: u64,         // quando o servidor de origem publicou no canal
    #[serde(default)]
    origin: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    clock: u64,
}

// Estado do servidor.
struct Server {
    store: Store,
    mensagens: Vec<MessageRec>, // cópia em memória do arquivo de publicações
    store_path: String,
    messages_path: String,
    publisher: zmq::Socket,
    nome: String,
    rank: u32,
    clock: u64,                  // relógio lógico
    offset_ms: i64,              // ajuste do relógio físico, recebido do coordenador
    coordenador: Option<String>, // nome do coordenador eleito (None = desconhecido)
    em_eleicao: bool,
    eleicao_pendente: bool, // recebeu pedido de eleição e precisa iniciar a sua
    aguardando_desde: Option<Instant>, // recebeu OK, esperando o anúncio do coordenador
    anuncio_enviado: Option<Instant>, // anunciou e o anúncio ainda não voltou pelo proxy
    // Só no coordenador: diferença entre o relógio de cada servidor e o seu (Berkeley)
    diferencas: HashMap<String, (i64, Instant)>,
    // Replicação: origem das escritas desta execução, último seq usado e os ids já guardados
    origem: String,
    seq: u64,
    possui: HashMap<String, BTreeSet<u64>>,
    proximo_sync: usize, // revezamento entre os servidores na anti-entropia
}

// Sockets e endereços para falar com a referência e com os outros servidores.
struct Rede {
    ctx: zmq::Context,
    ref_sock: zmq::Socket,
    ref_url: String,
    peer: zmq::Socket, // REP: pedidos de outros servidores (relógio, eleição e sincronização)
    sub: zmq::Socket,  // SUB: tópicos "servers" (coordenador) e "replica" (escritas)
    peer_host: String,
    peer_base: u32,
}

impl Rede {
    // Endereço do REP de outro servidor: o nome é "server<N>" e a porta é PEER_PORT_BASE + N.
    fn endereco(&self, nome: &str) -> Option<String> {
        let id: u32 = nome.strip_prefix("server")?.parse().ok()?;
        Some(format!("tcp://{}:{}", self.peer_host, self.peer_base + id))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// Hora ajustada pelo coordenador. Não mexe no relógio do container, só soma a diferença.
fn agora(offset_ms: i64) -> u64 {
    (now_ms() as i64 + offset_ms) as u64
}

// Regra: só letras, números e "_", entre min e max caracteres.
fn valid_name(name: &str, min: usize, max: usize) -> bool {
    let len = name.chars().count();
    (min..=max).contains(&len) && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn load(path: &str) -> Store {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Store::default(),
    };
    match serde_json::from_str(&text) {
        Ok(store) => store,
        Err(e) => {
            // Não perde o arquivo ilegível: guarda uma cópia antes de começar do zero
            let copia = format!("{}.invalido", path);
            eprintln!("erro ao ler {}: {} (cópia em {})", path, e, copia);
            let _ = fs::copy(path, copia);
            Store::default()
        }
    }
}

// Lê as publicações, uma por linha. Devolve também se o arquivo precisa ser regravado
// (linha inválida ou última linha sem quebra, por exemplo se o container caiu gravando).
fn load_mensagens(path: &str) -> (Vec<MessageRec>, bool) {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return (Vec::new(), false),
    };
    let mut mensagens = Vec::new();
    let mut regravar = !text.is_empty() && !text.ends_with('\n');
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(line) {
            Ok(m) => mensagens.push(m),
            Err(e) => {
                eprintln!("{}: linha {} ignorada ({})", path, i + 1, e);
                regravar = true;
            }
        }
    }
    if regravar {
        let _ = fs::copy(path, format!("{}.invalido", path));
    }
    (mensagens, regravar)
}

// Grava em arquivo temporário e renomeia, para não corromper o arquivo se o container cair.
fn save(store: &Store, path: &str) {
    let tmp = format!("{}.tmp", path);
    let text = serde_json::to_string_pretty(store).unwrap();
    if let Err(e) = fs::write(&tmp, text).and_then(|_| fs::rename(&tmp, path)) {
        eprintln!("erro ao salvar {}: {}", path, e);
    }
}

// Regrava o arquivo de publicações inteiro (só na migração ou depois de uma linha inválida).
fn save_mensagens(mensagens: &[MessageRec], path: &str) {
    let tmp = format!("{}.tmp", path);
    let mut text = String::new();
    for m in mensagens {
        text.push_str(&serde_json::to_string(m).unwrap());
        text.push('\n');
    }
    if let Err(e) = fs::write(&tmp, text).and_then(|_| fs::rename(&tmp, path)) {
        eprintln!("erro ao salvar {}: {}", path, e);
    }
}

// Acrescenta uma linha por publicação ao arquivo, sem reescrever as anteriores.
fn append(recs: &[MessageRec], path: &str) {
    if recs.is_empty() {
        return;
    }
    let mut text = String::new();
    for rec in recs {
        text.push_str(&serde_json::to_string(rec).unwrap());
        text.push('\n');
    }
    let result = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| f.write_all(text.as_bytes()));
    if let Err(e) = result {
        eprintln!("erro ao salvar {}: {}", path, e);
    }
}

// Registros gravados antes da replicação não têm origin/seq. Eles recebem a origem
// "<nome>@legado" e um seq na ordem dos arquivos, e os arquivos são regravados com os ids.
// Assim o histórico antigo de cada servidor também é replicado para os outros.
fn migrar_legado(srv: &mut Server, regravar_mensagens: bool) {
    let origem = format!("{}@legado", srv.nome);
    let mut seq = srv
        .store
        .logins
        .iter()
        .filter(|r| r.origin == origem)
        .map(|r| r.seq)
        .chain(
            srv.store
                .channels
                .iter()
                .filter(|r| r.origin == origem)
                .map(|r| r.seq),
        )
        .chain(
            srv.mensagens
                .iter()
                .filter(|r| r.origin == origem)
                .map(|r| r.seq),
        )
        .max()
        .unwrap_or(0);
    let inicio = seq;

    for r in srv.store.logins.iter_mut().filter(|r| r.origin.is_empty()) {
        seq += 1;
        r.origin = origem.clone();
        r.seq = seq;
    }
    for r in srv
        .store
        .channels
        .iter_mut()
        .filter(|r| r.origin.is_empty())
    {
        seq += 1;
        r.origin = origem.clone();
        r.seq = seq;
    }
    let antes_mensagens = seq;
    for r in srv.mensagens.iter_mut().filter(|r| r.origin.is_empty()) {
        seq += 1;
        r.origin = origem.clone();
        r.seq = seq;
    }

    if antes_mensagens > inicio {
        save(&srv.store, &srv.store_path);
    }
    if seq > antes_mensagens || regravar_mensagens {
        save_mensagens(&srv.mensagens, &srv.messages_path);
    }
    if seq > inicio {
        println!(
            "REPLICA   {} registros antigos migrados com a origem {}",
            seq - inicio,
            origem
        );
    }
}

// Monta o índice (origem -> seqs) de tudo que já está guardado.
fn indexar(srv: &mut Server) {
    let ids: Vec<(String, u64)> = srv
        .store
        .logins
        .iter()
        .map(|r| (r.origin.clone(), r.seq))
        .chain(srv.store.channels.iter().map(|r| (r.origin.clone(), r.seq)))
        .chain(srv.mensagens.iter().map(|r| (r.origin.clone(), r.seq)))
        .collect();
    for (origin, seq) in ids {
        srv.possui.entry(origin).or_default().insert(seq);
    }
}

fn login_record(r: &LoginRec) -> Record {
    Record {
        origin: r.origin.clone(),
        seq: r.seq,
        timestamp: r.timestamp,
        clock: r.clock,
        data: Some(record::Data::Login(LoginRecord {
            user: r.user.clone(),
        })),
    }
}

fn channel_record(r: &ChannelRec) -> Record {
    Record {
        origin: r.origin.clone(),
        seq: r.seq,
        timestamp: r.timestamp,
        clock: r.clock,
        data: Some(record::Data::Channel(ChannelRecord {
            name: r.name.clone(),
            created_by: r.created_by.clone(),
        })),
    }
}

fn message_record(r: &MessageRec) -> Record {
    Record {
        origin: r.origin.clone(),
        seq: r.seq,
        timestamp: r.timestamp,
        clock: r.clock,
        data: Some(record::Data::Message(MessageRecord {
            channel: r.channel.clone(),
            user: r.user.clone(),
            message: r.message.clone(),
            request_timestamp: r.request_timestamp,
        })),
    }
}

// Guarda um registro, criado aqui ou vindo de outro servidor. Como (origin, seq) é único,
// receber o mesmo registro de novo não muda nada. Devolve true se o registro era novo.
// Com persistir = false só guarda em memória: quem chamou grava o lote inteiro de uma vez.
fn aplicar(srv: &mut Server, rec: Record, persistir: bool) -> bool {
    let data = match rec.data {
        Some(data) if !rec.origin.is_empty() && rec.seq > 0 => data,
        _ => return false,
    };
    if !srv
        .possui
        .entry(rec.origin.clone())
        .or_default()
        .insert(rec.seq)
    {
        return false;
    }
    match data {
        record::Data::Login(l) => {
            srv.store.logins.push(LoginRec {
                user: l.user,
                timestamp: rec.timestamp,
                origin: rec.origin,
                seq: rec.seq,
                clock: rec.clock,
            });
            if persistir {
                save(&srv.store, &srv.store_path);
            }
        }
        record::Data::Channel(c) => {
            srv.store.channels.push(ChannelRec {
                name: c.name,
                created_by: c.created_by,
                timestamp: rec.timestamp,
                origin: rec.origin,
                seq: rec.seq,
                clock: rec.clock,
            });
            if persistir {
                save(&srv.store, &srv.store_path);
            }
        }
        record::Data::Message(m) => {
            let msg = MessageRec {
                channel: m.channel,
                user: m.user,
                message: m.message,
                request_timestamp: m.request_timestamp,
                timestamp: rec.timestamp,
                origin: rec.origin,
                seq: rec.seq,
                clock: rec.clock,
            };
            if persistir {
                append(std::slice::from_ref(&msg), &srv.messages_path);
            }
            srv.mensagens.push(msg);
        }
    }
    true
}

// Escrita aceita por este servidor: ganha o próximo seq desta origem, é guardada e
// publicada no tópico "replica" para os outros servidores (push).
fn escrever(srv: &mut Server, timestamp: u64, data: record::Data) {
    srv.seq += 1;
    let rec = Record {
        origin: srv.origem.clone(),
        seq: srv.seq,
        timestamp,
        clock: srv.clock,
        data: Some(data),
    };
    aplicar(srv, rec.clone(), true);

    srv.clock += 1;
    let msg = Replica {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        record: Some(rec),
    };
    let sent = srv
        .publisher
        .send(TOPICO_REPLICA.as_bytes(), zmq::SNDMORE)
        .and_then(|_| srv.publisher.send(msg.encode_to_vec(), 0));
    match sent {
        Ok(_) => println!("PUBLICADO [{}] {:?}", TOPICO_REPLICA, msg),
        // Os outros servidores recebem o registro depois, pela anti-entropia
        Err(e) => eprintln!("erro ao replicar: {}", e),
    }
}

// Escrita aceita por outro servidor, recebida pelo tópico "replica".
fn receber_replica(srv: &mut Server, dados: &[u8]) {
    let msg = match Replica::decode(dados) {
        Ok(m) => m,
        Err(_) => {
            println!("réplica inválida no tópico {}", TOPICO_REPLICA);
            return;
        }
    };
    srv.clock = srv.clock.max(msg.clock) + 1;
    let rec = match msg.record {
        // O próprio registro volta pelo proxy e já está guardado
        Some(ref r) if r.origin == srv.origem => return,
        Some(ref r) => r.clone(),
        None => return,
    };
    println!("RECEBIDO  [{}] {:?}", TOPICO_REPLICA, msg);
    let id = format!("{}#{}", rec.origin, rec.seq);
    if aplicar(srv, rec, true) {
        println!("REPLICA   registro {} guardado", id);
    }
}

// Vetor de versões: para cada origem, os intervalos de seq que este servidor já tem.
fn vetor_versoes(srv: &Server) -> Vec<Known> {
    srv.possui
        .iter()
        .map(|(origin, seqs)| {
            let mut ranges: Vec<SeqRange> = Vec::new();
            for &s in seqs {
                match ranges.last_mut() {
                    Some(r) if r.last + 1 == s => r.last = s,
                    _ => ranges.push(SeqRange { first: s, last: s }),
                }
            }
            Known {
                origin: origin.clone(),
                ranges,
            }
        })
        .collect()
}

// Responde à anti-entropia: todos os registros que quem pediu ainda não tem.
fn registros_faltando(srv: &Server, known: &[Known]) -> SyncReply {
    let mapa: HashMap<&str, &Vec<SeqRange>> = known
        .iter()
        .map(|k| (k.origin.as_str(), &k.ranges))
        .collect();
    let tem = |origin: &str, seq: u64| {
        mapa.get(origin).map_or(false, |rs| {
            rs.iter().any(|r| r.first <= seq && seq <= r.last)
        })
    };

    let mut faltam: Vec<Record> = Vec::new();
    for r in srv
        .store
        .logins
        .iter()
        .filter(|r| !tem(r.origin.as_str(), r.seq))
    {
        faltam.push(login_record(r));
    }
    for r in srv
        .store
        .channels
        .iter()
        .filter(|r| !tem(r.origin.as_str(), r.seq))
    {
        faltam.push(channel_record(r));
    }
    for r in srv
        .mensagens
        .iter()
        .filter(|r| !tem(r.origin.as_str(), r.seq))
    {
        faltam.push(message_record(r));
    }

    // Em ordem de origem e seq, para que quem pediu receba cada origem sem buracos
    faltam.sort_by(|a, b| (&a.origin, a.seq).cmp(&(&b.origin, b.seq)));
    let more = faltam.len() > LIMITE_SYNC;
    faltam.truncate(LIMITE_SYNC);
    SyncReply {
        records: faltam,
        more,
    }
}

// Pede a um servidor os registros que faltam, em rodadas, até não faltar mais nada.
fn sincronizar_dados(srv: &mut Server, rede: &Rede, nome: &str) {
    for _ in 0..MAX_RODADAS_SYNC {
        let body = peer_request::Body::Sync(SyncRequest {
            name: srv.nome.clone(),
            known: vetor_versoes(srv),
        });
        let resposta = match peer_call(srv, rede, nome, body) {
            Some(PeerReply {
                body: Some(peer_reply::Body::Sync(s)),
                ..
            }) => s,
            _ => return,
        };
        let total = resposta.records.len();
        let mut novos = 0;
        let mut store_mudou = false;
        let inicio = srv.mensagens.len();
        for rec in resposta.records {
            let e_mensagem = matches!(rec.data, Some(record::Data::Message(_)));
            if aplicar(srv, rec, false) {
                novos += 1;
                store_mudou |= !e_mensagem;
            }
        }
        // Grava o lote de uma vez: as publicações novas são as do fim da lista
        if store_mudou {
            save(&srv.store, &srv.store_path);
        }
        append(&srv.mensagens[inicio..], &srv.messages_path);
        if total > 0 {
            println!(
                "REPLICA   anti-entropia com {}: {} registros recebidos, {} novos",
                nome, total, novos
            );
        }
        // Para quando não falta mais nada (ou quando a rodada não trouxe nada novo)
        if !resposta.more || novos == 0 {
            return;
        }
    }
}

// Anti-entropia (pull). Na partida sincroniza com todos os servidores; depois, a cada
// INTERVALO_ANTI_ENTROPIA, com um servidor por vez, em revezamento.
fn anti_entropia(srv: &mut Server, rede: &mut Rede, todos: bool) {
    let mut outros: Vec<(u32, String)> =
        match ref_call(srv, rede, ref_request::Body::List(ListServers {})) {
            Some(RefReply {
                body: Some(ref_reply::Body::List(l)),
                ..
            }) => l
                .servers
                .into_iter()
                .filter(|s| s.name != srv.nome)
                .map(|s| (s.rank, s.name))
                .collect(),
            _ => return,
        };
    if outros.is_empty() {
        return;
    }
    outros.sort();

    if todos {
        for (_, nome) in &outros {
            sincronizar_dados(srv, rede, nome);
        }
    } else {
        let (_, nome) = &outros[srv.proximo_sync % outros.len()];
        srv.proximo_sync = srv.proximo_sync.wrapping_add(1);
        sincronizar_dados(srv, rede, nome);
    }
}

// Lista de canais sem repetição (o mesmo canal pode ter sido criado em dois servidores ao
// mesmo tempo), na ordem de criação: hora, relógio lógico e origem para desempate.
fn canais(srv: &Server) -> Vec<String> {
    let mut lista: Vec<&ChannelRec> = srv.store.channels.iter().collect();
    lista.sort_by(|a, b| {
        (a.timestamp, a.clock, &a.origin, a.seq).cmp(&(b.timestamp, b.clock, &b.origin, b.seq))
    });
    let mut nomes: Vec<String> = Vec::new();
    for c in lista {
        if !nomes.contains(&c.name) {
            nomes.push(c.name.clone());
        }
    }
    nomes
}

// Cria o socket REQ para a referência. Usado de novo se der timeout.
fn novo_ref(ctx: &zmq::Context, ref_url: &str) -> zmq::Socket {
    let sock = ctx.socket(zmq::REQ).unwrap();
    sock.set_linger(0).unwrap();
    sock.connect(ref_url).unwrap();
    sock
}

// Espera a resposta de um socket REQ e, enquanto isso, continua atendendo os pedidos de
// outros servidores. Assim dois servidores nunca ficam travados esperando um pelo outro.
fn esperar(
    srv: &mut Server,
    req: &zmq::Socket,
    peer: &zmq::Socket,
    timeout: Duration,
) -> Option<Vec<u8>> {
    let limite = Instant::now() + timeout;
    loop {
        let resto = limite.saturating_duration_since(Instant::now());
        if resto.is_zero() {
            return None;
        }
        let mut items = [
            req.as_poll_item(zmq::POLLIN),
            peer.as_poll_item(zmq::POLLIN),
        ];
        zmq::poll(&mut items, resto.as_millis() as i64 + 1).ok()?;
        let (resposta, pedido) = (items[0].is_readable(), items[1].is_readable());
        if pedido {
            atender_peer(srv, peer);
        }
        if resposta {
            return req.recv_bytes(0).ok();
        }
    }
}

// Faz uma requisição à referência e aplica o relógio lógico na resposta.
fn ref_call(srv: &mut Server, rede: &mut Rede, body: ref_request::Body) -> Option<RefReply> {
    srv.clock += 1;
    let req = RefRequest {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        body: Some(body),
    };
    if rede.ref_sock.send(&req.encode_to_vec()[..], 0).is_err() {
        return None;
    }
    let raw = match esperar(srv, &rede.ref_sock, &rede.peer, TIMEOUT_REF) {
        Some(raw) => raw,
        None => {
            // Timeout: o REQ fica inválido, então recria o socket
            rede.ref_sock = novo_ref(&rede.ctx, &rede.ref_url);
            return None;
        }
    };
    let rep = RefReply::decode(&raw[..]).ok()?;
    srv.clock = srv.clock.max(rep.clock) + 1;
    Some(rep)
}

// Texto da resposta para o log. A resposta da anti-entropia pode ter centenas de
// registros, então mostra só a quantidade.
fn resumo(rep: &PeerReply) -> String {
    match &rep.body {
        Some(peer_reply::Body::Sync(s)) => format!(
            "PeerReply {{ timestamp: {}, clock: {}, body: Some(Sync({} registros, more: {})) }}",
            rep.timestamp,
            rep.clock,
            s.records.len(),
            s.more
        ),
        _ => format!("{:?}", rep),
    }
}

// Faz uma requisição a outro servidor. Devolve None se ele não responder a tempo.
fn peer_call(
    srv: &mut Server,
    rede: &Rede,
    nome: &str,
    body: peer_request::Body,
) -> Option<PeerReply> {
    let url = rede.endereco(nome)?;
    // Um REQ novo por pedido: se o outro servidor caiu, basta descartar o socket
    let sock = rede.ctx.socket(zmq::REQ).ok()?;
    sock.set_linger(0).ok()?;
    sock.connect(&url).ok()?;

    srv.clock += 1;
    let req = PeerRequest {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        body: Some(body),
    };
    println!("ENVIADO   [{}] {:?}", nome, req);
    sock.send(&req.encode_to_vec()[..], 0).ok()?;

    let raw = match esperar(srv, &sock, &rede.peer, TIMEOUT_PEER) {
        Some(raw) => raw,
        None => {
            println!("sem resposta de {}", nome);
            return None;
        }
    };
    let rep = PeerReply::decode(&raw[..]).ok()?;
    srv.clock = srv.clock.max(rep.clock) + 1;
    println!("RECEBIDO  [{}] {}", nome, resumo(&rep));
    Some(rep)
}

// Pede o rank à referência. Repete até conseguir.
fn cadastrar(srv: &mut Server, rede: &mut Rede) {
    loop {
        let body = ref_request::Body::Register(Register {
            name: srv.nome.clone(),
        });
        if let Some(RefReply {
            body: Some(ref_reply::Body::Register(r)),
            ..
        }) = ref_call(srv, rede, body)
        {
            srv.rank = r.rank;
            println!("cadastrado na referência com rank {}", r.rank);
            return;
        }
        println!("referência indisponível, tentando de novo...");
        thread::sleep(Duration::from_secs(2));
    }
}

// Avisa à referência que está vivo. A hora não vem mais daqui, e sim do coordenador.
fn heartbeat(srv: &mut Server, rede: &mut Rede) {
    let body = ref_request::Body::Heartbeat(Heartbeat {
        name: srv.nome.clone(),
    });
    let rep = match ref_call(srv, rede, body) {
        Some(r) => r,
        None => {
            println!("heartbeat sem resposta");
            return;
        }
    };
    if let Some(ref_reply::Body::Heartbeat(hb)) = rep.body {
        if hb.status == "NAO_CADASTRADO" {
            cadastrar(srv, rede);
            return;
        }
        println!("heartbeat OK");
    }
}

// Algoritmo do valentão: pede eleição a todos os servidores de rank maior. Se algum responder
// OK, ele assume a eleição e este servidor espera o anúncio. Se nenhum responder, este
// servidor vira o coordenador e avisa os outros pelo tópico "servers".
fn eleicao(srv: &mut Server, rede: &mut Rede) {
    srv.em_eleicao = true;
    srv.eleicao_pendente = false;
    println!("ELEIÇÃO   iniciada por {} (rank {})", srv.nome, srv.rank);

    let servidores = match ref_call(srv, rede, ref_request::Body::List(ListServers {})) {
        Some(RefReply {
            body: Some(ref_reply::Body::List(l)),
            ..
        }) => l.servers,
        _ => {
            // Sem a lista não dá para saber quem tem rank maior: tenta de novo depois
            println!("ELEIÇÃO   referência indisponível, eleição adiada");
            srv.em_eleicao = false;
            srv.eleicao_pendente = true;
            return;
        }
    };

    let maiores: Vec<String> = servidores
        .into_iter()
        .filter(|s| s.rank > srv.rank && s.name != srv.nome)
        .map(|s| s.name)
        .collect();

    let mut recebeu_ok = false;
    for nome in &maiores {
        let body = peer_request::Body::Election(Election {
            name: srv.nome.clone(),
            rank: srv.rank,
        });
        if let Some(PeerReply {
            body: Some(peer_reply::Body::Election(r)),
            ..
        }) = peer_call(srv, rede, nome, body)
        {
            if r.status == "OK" {
                recebeu_ok = true;
            }
        }
    }

    srv.em_eleicao = false;
    if recebeu_ok {
        println!("ELEIÇÃO   recebeu OK, aguardando o anúncio do coordenador");
        srv.coordenador = None;
        srv.aguardando_desde = Some(Instant::now());
    } else {
        virar_coordenador(srv);
    }
}

// Assume como coordenador e publica [servers] [nome do coordenador].
fn virar_coordenador(srv: &mut Server) {
    if srv.coordenador.as_deref() != Some(srv.nome.as_str()) {
        srv.diferencas.clear();
    }
    srv.coordenador = Some(srv.nome.clone());
    srv.aguardando_desde = None;
    // O servidor também é inscrito em "servers": quando o anúncio voltar, chegou ao proxy
    srv.anuncio_enviado = Some(Instant::now());

    srv.clock += 1;
    let msg = Coordinator {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        name: srv.nome.clone(),
    };
    let sent = srv
        .publisher
        .send(TOPICO_SERVERS.as_bytes(), zmq::SNDMORE)
        .and_then(|_| srv.publisher.send(msg.encode_to_vec(), 0));
    match sent {
        Ok(_) => println!("PUBLICADO [{}] {:?}", TOPICO_SERVERS, msg),
        Err(e) => eprintln!("erro ao anunciar coordenador: {}", e),
    }
    println!("ELEIÇÃO   {} é o novo coordenador", srv.nome);
}

// Lê tudo o que chegou no SUB e separa pelo tópico.
fn receber_sub(srv: &mut Server, sub: &zmq::Socket) {
    while let Ok(partes) = sub.recv_multipart(zmq::DONTWAIT) {
        // O filtro do ZeroMQ é por prefixo, então confere o tópico exato
        if partes.len() != 2 {
            continue;
        }
        if partes[0] == TOPICO_SERVERS.as_bytes() {
            receber_anuncio(srv, &partes[1]);
        } else if partes[0] == TOPICO_REPLICA.as_bytes() {
            receber_replica(srv, &partes[1]);
        }
    }
}

// Recebe o anúncio de um novo coordenador no tópico "servers".
fn receber_anuncio(srv: &mut Server, dados: &[u8]) {
    let msg = match Coordinator::decode(dados) {
        Ok(m) => m,
        Err(_) => {
            println!("anúncio inválido no tópico {}", TOPICO_SERVERS);
            return;
        }
    };
    srv.clock = srv.clock.max(msg.clock) + 1;
    println!("RECEBIDO  [{}] {:?}", TOPICO_SERVERS, msg);

    if msg.name == srv.nome {
        // O próprio anúncio voltou: foi entregue a quem está inscrito no tópico
        srv.anuncio_enviado = None;
        return;
    }
    if srv.coordenador.as_deref() == Some(srv.nome.as_str()) {
        // Dois coordenadores ao mesmo tempo: uma nova eleição decide (o maior rank vence)
        println!(
            "ELEIÇÃO   {} também se anunciou coordenador, pedindo nova eleição",
            msg.name
        );
        srv.eleicao_pendente = true;
    }
    srv.diferencas.clear();
    srv.coordenador = Some(msg.name);
    srv.aguardando_desde = None;
    srv.anuncio_enviado = None;
    println!(
        "coordenador atual: {}",
        srv.coordenador.as_deref().unwrap_or("")
    );
}

// Algoritmo de Berkeley no coordenador: guarda a diferença entre o relógio de quem pediu e o
// seu, tira a média das diferenças conhecidas (o próprio coordenador entra com 0), ajusta o
// próprio relógio para a média e devolve essa hora como a hora correta.
fn berkeley(srv: &mut Server, nome: &str, hora_servidor: u64) -> u64 {
    let diferenca = hora_servidor as i64 - agora(srv.offset_ms) as i64;
    srv.diferencas
        .insert(nome.to_string(), (diferenca, Instant::now()));
    srv.diferencas
        .retain(|_, (_, quando)| quando.elapsed() <= VALIDADE_BERKELEY);

    let soma: i64 = srv.diferencas.values().map(|(d, _)| d).sum();
    let media = soma / (srv.diferencas.len() as i64 + 1);

    // O coordenador anda a média; os outros relógios ficam a (diferença - média) dele
    srv.offset_ms += media;
    for (d, _) in srv.diferencas.values_mut() {
        *d -= media;
    }
    // Quem pediu vai adotar a hora devolvida, então passa a ter diferença 0
    srv.diferencas.insert(nome.to_string(), (0, Instant::now()));

    println!(
        "BERKELEY  {} estava {} ms do coordenador | média {} ms | ajuste do coordenador: {} ms",
        nome, diferenca, media, srv.offset_ms
    );
    agora(srv.offset_ms)
}

// Atende um pedido de outro servidor no socket REP.
fn atender_peer(srv: &mut Server, peer: &zmq::Socket) {
    let raw = match peer.recv_bytes(0) {
        Ok(raw) => raw,
        Err(_) => return,
    };
    let reply = handle_peer(&raw, srv);
    if let Err(e) = peer.send(&reply[..], 0) {
        eprintln!("erro ao responder servidor: {}", e);
    }
}

fn handle_peer(raw: &[u8], srv: &mut Server) -> Vec<u8> {
    let req = match PeerRequest::decode(raw) {
        Ok(r) => r,
        Err(_) => {
            println!("mensagem inválida ({} bytes)", raw.len());
            return Vec::new();
        }
    };

    srv.clock = srv.clock.max(req.clock) + 1;
    println!("RECEBIDO  {:?}", req);

    let body = match req.body {
        // REQ: relógio -> REP: hora correta (só o coordenador responde com a hora)
        Some(peer_request::Body::ClockRequest(c)) => {
            let reply = if srv.coordenador.as_deref() == Some(srv.nome.as_str()) {
                ClockReply {
                    status: "OK".to_string(),
                    time_ms: berkeley(srv, &c.name, req.timestamp),
                }
            } else {
                ClockReply {
                    status: "NAO_COORDENADOR".to_string(),
                    time_ms: 0,
                }
            };
            Some(peer_reply::Body::ClockReply(reply))
        }

        // REQ: eleição -> REP: OK. Quem tem rank maior assume e faz a sua própria eleição.
        Some(peer_request::Body::Election(e)) => {
            println!("ELEIÇÃO   pedido de {} (rank {})", e.name, e.rank);
            if !srv.em_eleicao {
                srv.eleicao_pendente = true;
            }
            Some(peer_reply::Body::Election(ElectionReply {
                status: "OK".to_string(),
            }))
        }

        // REQ: o que eu tenho -> REP: os registros que faltam (anti-entropia)
        Some(peer_request::Body::Sync(s)) => {
            Some(peer_reply::Body::Sync(registros_faltando(srv, &s.known)))
        }

        None => None,
    };

    srv.clock += 1;
    let reply = PeerReply {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        body,
    };
    println!("ENVIADO   {}", resumo(&reply));
    reply.encode_to_vec()
}

// Pede a hora correta ao coordenador. Se ele não estiver disponível, pede eleição.
fn sincronizar(srv: &mut Server, rede: &mut Rede) {
    let coordenador = match srv.coordenador.clone() {
        Some(c) => c,
        None => {
            // Já recebeu OK de alguém: o anúncio ainda vai chegar
            if srv.aguardando_desde.is_none() {
                eleicao(srv, rede);
            }
            return;
        }
    };
    if coordenador == srv.nome {
        println!(
            "sincronização: {} é o coordenador, o seu relógio é a referência",
            srv.nome
        );
        return;
    }

    let body = peer_request::Body::ClockRequest(ClockRequest {
        name: srv.nome.clone(),
    });
    let inicio = now_ms();
    match peer_call(srv, rede, &coordenador, body) {
        Some(PeerReply {
            body: Some(peer_reply::Body::ClockReply(c)),
            ..
        }) if c.status == "OK" => {
            // Soma metade do tempo de ida e volta, que é o atraso estimado da resposta
            let fim = now_ms();
            let rtt = fim - inicio;
            let novo = (c.time_ms + rtt / 2) as i64 - fim as i64;
            println!(
                "relógio sincronizado com {}: ajuste de {} ms (rtt {} ms)",
                coordenador,
                novo - srv.offset_ms,
                rtt
            );
            srv.offset_ms = novo;
        }
        _ => {
            println!("coordenador {} indisponível, pedindo eleição", coordenador);
            eleicao(srv, rede);
        }
    }
}

fn handle(raw: &[u8], srv: &mut Server) -> Vec<u8> {
    let req = match Request::decode(raw) {
        Ok(r) => r,
        Err(_) => {
            println!("mensagem inválida ({} bytes)", raw.len());
            return Vec::new();
        }
    };

    // Relógio lógico: maior valor entre o seu e o recebido, depois incrementa
    srv.clock = srv.clock.max(req.clock) + 1;
    println!("RECEBIDO  {:?}", req);

    let offset = srv.offset_ms;
    let req_timestamp = req.timestamp;

    let body = match req.body {
        Some(request::Body::Login(login)) => {
            let status = if valid_name(&login.user, 3, 20) {
                escrever(
                    srv,
                    agora(offset),
                    record::Data::Login(LoginRecord {
                        user: login.user.clone(),
                    }),
                );
                println!("login: {}", login.user);
                "OK"
            } else {
                "INVALID_USERNAME"
            };
            reply::Body::Login(LoginReply {
                status: status.to_string(),
            })
        }

        Some(request::Body::ListChannels(_)) => reply::Body::Channels(ChannelList {
            status: "OK".to_string(),
            channels: canais(srv),
        }),

        Some(request::Body::CreateChannel(create)) => {
            // "servers" e "replica" são tópicos reservados para a comunicação entre servidores
            let reservado = create.name == TOPICO_SERVERS || create.name == TOPICO_REPLICA;
            let status = if !valid_name(&create.name, 3, 30) || reservado {
                "INVALID_CHANNEL"
            } else if srv.store.channels.iter().any(|c| c.name == create.name) {
                "CHANNEL_EXISTS"
            } else {
                escrever(
                    srv,
                    agora(offset),
                    record::Data::Channel(ChannelRecord {
                        name: create.name.clone(),
                        created_by: create.user.clone(),
                    }),
                );
                println!("canal criado: {} por {}", create.name, create.user);
                "OK"
            };
            reply::Body::CreateChannel(CreateChannelReply {
                status: status.to_string(),
            })
        }

        Some(request::Body::Publish(publish)) => {
            let len = publish.message.chars().count();
            let status = if !srv.store.channels.iter().any(|c| c.name == publish.channel) {
                "CHANNEL_NOT_FOUND"
            } else if len == 0 || len > 500 {
                "INVALID_MESSAGE"
            } else {
                srv.clock += 1;
                let msg = ChannelMessage {
                    timestamp: agora(offset),
                    channel: publish.channel.clone(),
                    user: publish.user.clone(),
                    message: publish.message.clone(),
                    clock: srv.clock,
                };
                // Mensagem em 2 partes: [tópico = nome do canal, ChannelMessage serializada]
                let sent = srv
                    .publisher
                    .send(msg.channel.as_bytes(), zmq::SNDMORE)
                    .and_then(|_| srv.publisher.send(msg.encode_to_vec(), 0));
                match sent {
                    Ok(_) => {
                        println!("PUBLICADO [{}] {:?}", msg.channel, msg);
                        // Mesma hora e relógio lógico da publicação no canal
                        escrever(
                            srv,
                            msg.timestamp,
                            record::Data::Message(MessageRecord {
                                channel: msg.channel,
                                user: msg.user,
                                message: msg.message,
                                request_timestamp: req_timestamp,
                            }),
                        );
                        "OK"
                    }
                    Err(e) => {
                        eprintln!("erro ao publicar: {}", e);
                        "PUBLISH_ERROR"
                    }
                }
            };
            reply::Body::Publish(PublishReply {
                status: status.to_string(),
            })
        }

        // Mensagem desconhecida ou vazia: responde com lista vazia e status de erro.
        None => reply::Body::Channels(ChannelList {
            status: "INVALID_REQUEST".to_string(),
            channels: vec![],
        }),
    };

    srv.clock += 1;
    let reply = Reply {
        timestamp: agora(offset),
        clock: srv.clock,
        body: Some(body),
    };
    println!("ENVIADO   {:?}", reply);
    reply.encode_to_vec()
}

fn main() {
    let server_id = env::var("SERVER_ID").unwrap_or_else(|_| "1".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or_else(|_| "tcp://broker:5556".to_string());
    let proxy_url = env::var("PROXY_URL").unwrap_or_else(|_| "tcp://proxy:5557".to_string());
    let ref_url = env::var("REF_URL").unwrap_or_else(|_| "tcp://reference:5559".to_string());
    // Os padrões abaixo seguem o docker-compose (network_mode: host, tudo em localhost)
    let sub_url = env::var("PROXY_SUB_URL").unwrap_or_else(|_| "tcp://localhost:5558".to_string());
    let peer_host = env::var("PEER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let peer_base: u32 = env::var("PEER_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5600);
    // Diferença inicial do relógio físico, para testar a sincronização (padrão 0)
    let offset_inicial: i64 = env::var("CLOCK_OFFSET_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let id: u32 = server_id.parse().expect("SERVER_ID deve ser um número");
    let store_path = format!("/app/data/server_{}.json", server_id);
    let messages_path = format!("/app/data/server_{}_messages.jsonl", server_id);

    if let Some(dir) = Path::new(&store_path).parent() {
        fs::create_dir_all(dir).unwrap();
    }

    let ctx = zmq::Context::new();

    let sock = ctx.socket(zmq::REP).unwrap();
    sock.connect(&broker_url).unwrap();

    let publisher = ctx.socket(zmq::PUB).unwrap();
    publisher.connect(&proxy_url).unwrap();

    // Pedidos de outros servidores (relógio, eleição e sincronização)
    let peer_url = format!("tcp://*:{}", peer_base + id);
    let peer = ctx.socket(zmq::REP).unwrap();
    peer.bind(&peer_url).unwrap();

    // Anúncios do coordenador e réplicas das escritas, publicados pelos servidores no proxy
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.connect(&sub_url).unwrap();
    sub.set_subscribe(TOPICO_SERVERS.as_bytes()).unwrap();
    sub.set_subscribe(TOPICO_REPLICA.as_bytes()).unwrap();

    let mut rede = Rede {
        ref_sock: novo_ref(&ctx, &ref_url),
        ctx,
        ref_url,
        peer,
        sub,
        peer_host,
        peer_base,
    };

    let nome = format!("server{}", server_id);
    let (mensagens, regravar_mensagens) = load_mensagens(&messages_path);
    let mut srv = Server {
        store: load(&store_path),
        mensagens,
        store_path,
        messages_path,
        publisher,
        // Cada execução é uma origem nova, então um seq nunca é reutilizado após reiniciar
        origem: format!("{}@{}", nome, now_ms()),
        nome,
        rank: 0,
        clock: 0,
        offset_ms: offset_inicial,
        coordenador: None,
        em_eleicao: false,
        eleicao_pendente: false,
        aguardando_desde: None,
        anuncio_enviado: None,
        diferencas: HashMap::new(),
        seq: 0,
        possui: HashMap::new(),
        proximo_sync: 0,
    };
    migrar_legado(&mut srv, regravar_mensagens);
    indexar(&mut srv);

    println!(
        "servidor {} conectado em {} (req/rep), {} (pub), {} (sub [{}, {}]), referência em {} e servidores em {}",
        server_id, broker_url, proxy_url, sub_url, TOPICO_SERVERS, TOPICO_REPLICA, rede.ref_url, peer_url
    );
    println!(
        "REPLICA   origem {} | {} logins, {} canais e {} publicações carregados do disco",
        srv.origem,
        srv.store.logins.len(),
        srv.store.channels.len(),
        srv.mensagens.len()
    );

    cadastrar(&mut srv, &mut rede);
    // Quem entra (ou volta) pede eleição para descobrir ou se tornar o coordenador
    eleicao(&mut srv, &mut rede);
    // Antes de atender os bots, busca nos outros servidores tudo o que perdeu enquanto estava fora
    anti_entropia(&mut srv, &mut rede, true);
    println!(
        "REPLICA   pronto com {} logins, {} canais e {} publicações",
        srv.store.logins.len(),
        srv.store.channels.len(),
        srv.mensagens.len()
    );

    let mut desde_heartbeat = 0;
    let mut desde_sync = 0;
    let mut ultimo_heartbeat = Instant::now();
    let mut ultima_anti_entropia = Instant::now();

    loop {
        let mut items = [
            sock.as_poll_item(zmq::POLLIN),
            rede.peer.as_poll_item(zmq::POLLIN),
            rede.sub.as_poll_item(zmq::POLLIN),
        ];
        zmq::poll(&mut items, 1000).unwrap();
        let (cliente, pedido, inscricao) = (
            items[0].is_readable(),
            items[1].is_readable(),
            items[2].is_readable(),
        );

        if inscricao {
            receber_sub(&mut srv, &rede.sub);
        }
        if pedido {
            atender_peer(&mut srv, &rede.peer);
        }
        if cliente {
            let raw = sock.recv_bytes(0).unwrap();
            let reply = handle(&raw, &mut srv);
            sock.send(&reply[..], 0).unwrap();

            // Relógio sincronizado com o coordenador a cada 15 mensagens de clientes
            desde_heartbeat += 1;
            desde_sync += 1;
            if desde_sync >= MSGS_SYNC {
                desde_sync = 0;
                sincronizar(&mut srv, &mut rede);
            }
        }

        // Heartbeat a cada 10 mensagens de clientes ou a cada 5 s
        if desde_heartbeat >= MSGS_HEARTBEAT || ultimo_heartbeat.elapsed() >= INTERVALO_HEARTBEAT {
            desde_heartbeat = 0;
            ultimo_heartbeat = Instant::now();
            heartbeat(&mut srv, &mut rede);
        }

        // Anti-entropia: recupera o que se perdeu no Pub/Sub ou enquanto um servidor estava fora
        if ultima_anti_entropia.elapsed() >= INTERVALO_ANTI_ENTROPIA {
            ultima_anti_entropia = Instant::now();
            anti_entropia(&mut srv, &mut rede, false);
        }

        if srv
            .anuncio_enviado
            .map_or(false, |t| t.elapsed() > REENVIO_ANUNCIO)
        {
            println!("ELEIÇÃO   anúncio não voltou pelo proxy, publicando de novo");
            virar_coordenador(&mut srv);
        }

        if srv.eleicao_pendente {
            eleicao(&mut srv, &mut rede);
        } else if srv.coordenador.is_none()
            && srv
                .aguardando_desde
                .map_or(false, |t| t.elapsed() > ESPERA_ANUNCIO)
        {
            println!("ELEIÇÃO   nenhum coordenador anunciado, pedindo nova eleição");
            eleicao(&mut srv, &mut rede);
        }
    }
}
