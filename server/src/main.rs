use prost::Message;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/chat.rs"));
}

use proto::{
    ref_reply, ref_request, reply, request, ChannelList, ChannelMessage, CreateChannelReply,
    Heartbeat, LoginReply, PublishReply, RefReply, RefRequest, Register, Reply, Request,
};

// Dados persistidos em data/server_<id>.json (um arquivo por servidor).
#[derive(Serialize, Deserialize, Default)]
struct Store {
    logins: Vec<LoginRec>,
    channels: Vec<ChannelRec>,
}

#[derive(Serialize, Deserialize)]
struct LoginRec {
    user: String,
    timestamp: u64,
}

#[derive(Serialize, Deserialize)]
struct ChannelRec {
    name: String,
    created_by: String,
    timestamp: u64,
}

// Publicações ficam em data/server_<id>_messages.jsonl (uma linha JSON por publicação).
#[derive(Serialize, Deserialize)]
struct MessageRec {
    channel: String,
    user: String,
    message: String,
    request_timestamp: u64, // quando o bot enviou o pedido
    timestamp: u64,         // quando o servidor publicou no canal
}

// Estado do servidor.
struct Server {
    store: Store,
    store_path: String,
    messages_path: String,
    publisher: zmq::Socket,
    nome: String,
    rank: u32,
    clock: u64,      // relógio lógico
    offset_ms: i64,  // diferença entre a hora da referência e a deste servidor
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// Hora ajustada pelo heartbeat. Não mexe no relógio do container, só soma a diferença.
fn agora(offset_ms: i64) -> u64 {
    (now_ms() as i64 + offset_ms) as u64
}

// Regra: só letras, números e "_", entre min e max caracteres.
fn valid_name(name: &str, min: usize, max: usize) -> bool {
    let len = name.chars().count();
    (min..=max).contains(&len) && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn load(path: &str) -> Store {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => Store::default(),
    }
}

// Grava em arquivo temporário e renomeia, para não corromper o arquivo se o container cair.
fn save(store: &Store, path: &str) {
    let tmp = format!("{}.tmp", path);
    let text = serde_json::to_string_pretty(store).unwrap();
    if let Err(e) = fs::write(&tmp, text).and_then(|_| fs::rename(&tmp, path)) {
        eprintln!("erro ao salvar {}: {}", path, e);
    }
}

// Acrescenta uma linha ao arquivo, sem reescrever as publicações anteriores.
fn append(rec: &MessageRec, path: &str) {
    let line = serde_json::to_string(rec).unwrap();
    let result = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| writeln!(f, "{}", line));
    if let Err(e) = result {
        eprintln!("erro ao salvar {}: {}", path, e);
    }
}

// Cria o socket REQ para a referência. Usado de novo se der timeout.
fn novo_ref(ctx: &zmq::Context, ref_url: &str) -> zmq::Socket {
    let sock = ctx.socket(zmq::REQ).unwrap();
    sock.set_rcvtimeo(3000).unwrap();
    sock.set_linger(0).unwrap();
    sock.connect(ref_url).unwrap();
    sock
}

// Faz uma requisição à referência e aplica o relógio lógico na resposta.
fn ref_call(
    srv: &mut Server,
    ref_sock: &mut zmq::Socket,
    ctx: &zmq::Context,
    ref_url: &str,
    body: ref_request::Body,
) -> Option<RefReply> {
    srv.clock += 1;
    let req = RefRequest {
        timestamp: agora(srv.offset_ms),
        clock: srv.clock,
        body: Some(body),
    };
    if ref_sock.send(&req.encode_to_vec()[..], 0).is_err() {
        return None;
    }
    let raw = match ref_sock.recv_bytes(0) {
        Ok(raw) => raw,
        Err(_) => {
            // Timeout: o REQ fica inválido, então recria o socket
            *ref_sock = novo_ref(ctx, ref_url);
            return None;
        }
    };
    let rep = RefReply::decode(&raw[..]).ok()?;
    srv.clock = srv.clock.max(rep.clock) + 1;
    Some(rep)
}

// Pede o rank à referência. Repete até conseguir.
fn cadastrar(srv: &mut Server, ref_sock: &mut zmq::Socket, ctx: &zmq::Context, ref_url: &str) {
    loop {
        let body = ref_request::Body::Register(Register {
            name: srv.nome.clone(),
        });
        if let Some(RefReply {
            body: Some(ref_reply::Body::Register(r)),
            ..
        }) = ref_call(srv, ref_sock, ctx, ref_url, body)
        {
            srv.rank = r.rank;
            println!("cadastrado na referência com rank {}", r.rank);
            return;
        }
        println!("referência indisponível, tentando de novo...");
        thread::sleep(Duration::from_secs(2));
    }
}

// Avisa à referência que está vivo e atualiza o ajuste do relógio físico.
fn heartbeat(srv: &mut Server, ref_sock: &mut zmq::Socket, ctx: &zmq::Context, ref_url: &str) {
    let body = ref_request::Body::Heartbeat(Heartbeat {
        name: srv.nome.clone(),
    });
    let rep = match ref_call(srv, ref_sock, ctx, ref_url, body) {
        Some(r) => r,
        None => {
            println!("heartbeat sem resposta");
            return;
        }
    };
    if let Some(ref_reply::Body::Heartbeat(hb)) = rep.body {
        if hb.status == "NAO_CADASTRADO" {
            cadastrar(srv, ref_sock, ctx, ref_url);
            return;
        }
        srv.offset_ms = hb.time_ms as i64 - now_ms() as i64;
        println!("heartbeat OK | ajuste de relógio: {} ms", srv.offset_ms);
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
                srv.store.logins.push(LoginRec {
                    user: login.user.clone(),
                    timestamp: agora(offset),
                });
                save(&srv.store, &srv.store_path);
                println!("login: {}", login.user);
                "OK"
            } else {
                "INVALID_USERNAME"
            };
            reply::Body::Login(LoginReply {
                status: status.to_string(),
            })
        }

        Some(request::Body::ListChannels(_)) => {
            let names = srv.store.channels.iter().map(|c| c.name.clone()).collect();
            reply::Body::Channels(ChannelList {
                status: "OK".to_string(),
                channels: names,
            })
        }

        Some(request::Body::CreateChannel(create)) => {
            let status = if !valid_name(&create.name, 3, 30) {
                "INVALID_CHANNEL"
            } else if srv.store.channels.iter().any(|c| c.name == create.name) {
                "CHANNEL_EXISTS"
            } else {
                srv.store.channels.push(ChannelRec {
                    name: create.name.clone(),
                    created_by: create.user.clone(),
                    timestamp: agora(offset),
                });
                save(&srv.store, &srv.store_path);
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
                        append(
                            &MessageRec {
                                channel: msg.channel,
                                user: msg.user,
                                message: msg.message,
                                request_timestamp: req_timestamp,
                                timestamp: msg.timestamp,
                            },
                            &srv.messages_path,
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

    let mut ref_sock = novo_ref(&ctx, &ref_url);

    let mut srv = Server {
        store: load(&store_path),
        store_path,
        messages_path,
        publisher,
        nome: format!("server{}", server_id),
        rank: 0,
        clock: 0,
        offset_ms: 0,
    };

    println!(
        "servidor {} conectado em {} (req/rep), {} (pub) e referência em {}",
        server_id, broker_url, proxy_url, ref_url
    );

    cadastrar(&mut srv, &mut ref_sock, &ctx, &ref_url);

    // Heartbeat a cada 10 mensagens de clientes
    let mut desde_heartbeat = 0;

    loop {
        let raw = sock.recv_bytes(0).unwrap();
        let reply = handle(&raw, &mut srv);
        sock.send(&reply[..], 0).unwrap();

        desde_heartbeat += 1;
        if desde_heartbeat >= 10 {
            desde_heartbeat = 0;
            heartbeat(&mut srv, &mut ref_sock, &ctx, &ref_url);
        }
    }
}