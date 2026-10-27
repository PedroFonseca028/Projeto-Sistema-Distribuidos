use prost::Message;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/chat.rs"));
}

use proto::{
    reply, request, ChannelList, ChannelMessage, CreateChannelReply, LoginReply, PublishReply,
    Reply, Request,
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

// Estado do servidor: dados em memória, arquivos de persistência e socket PUB.
struct Server {
    store: Store,
    store_path: String,
    messages_path: String,
    publisher: zmq::Socket,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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

fn handle(raw: &[u8], srv: &mut Server) -> Vec<u8> {
    let req = Request::decode(raw).ok();
    match &req {
        Some(r) => println!("RECEBIDO  {:?}", r),
        None => println!("RECEBIDO  mensagem inválida ({} bytes)", raw.len()),
    }
    let req_timestamp = req.as_ref().map_or(0, |r| r.timestamp);
    let store = &mut srv.store;

    let reply_body = match req.and_then(|r| r.body) {
        Some(request::Body::Login(login)) => {
            let status = if valid_name(&login.user, 3, 20) {
                store.logins.push(LoginRec {
                    user: login.user.clone(),
                    timestamp: now_ms(),
                });
                save(store, &srv.store_path);
                "OK"
            } else {
                "INVALID_USERNAME"
            };
            reply::Body::Login(LoginReply {
                status: status.to_string(),
            })
        }

        Some(request::Body::ListChannels(_)) => {
            let names = store.channels.iter().map(|c| c.name.clone()).collect();
            reply::Body::Channels(ChannelList {
                status: "OK".to_string(),
                channels: names,
            })
        }

        Some(request::Body::CreateChannel(create)) => {
            let status = if !valid_name(&create.name, 3, 30) {
                "INVALID_CHANNEL"
            } else if store.channels.iter().any(|c| c.name == create.name) {
                "CHANNEL_EXISTS"
            } else {
                store.channels.push(ChannelRec {
                    name: create.name.clone(),
                    created_by: create.user.clone(),
                    timestamp: now_ms(),
                });
                save(store, &srv.store_path);
                "OK"
            };
            reply::Body::CreateChannel(CreateChannelReply {
                status: status.to_string(),
            })
        }

        Some(request::Body::Publish(publish)) => {
            let len = publish.message.chars().count();
            let status = if !store.channels.iter().any(|c| c.name == publish.channel) {
                "CHANNEL_NOT_FOUND"
            } else if len == 0 || len > 500 {
                "INVALID_MESSAGE"
            } else {
                let msg = ChannelMessage {
                    timestamp: now_ms(),
                    channel: publish.channel.clone(),
                    user: publish.user.clone(),
                    message: publish.message.clone(),
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

    let reply = Reply {
        timestamp: now_ms(),
        body: Some(reply_body),
    };
    println!("ENVIADO   {:?}", reply);
    reply.encode_to_vec()
}

fn main() {
    let server_id = env::var("SERVER_ID").unwrap_or_else(|_| "1".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or_else(|_| "tcp://broker:5556".to_string());
    let proxy_url = env::var("PROXY_URL").unwrap_or_else(|_| "tcp://proxy:5557".to_string());
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

    println!(
        "servidor {} conectado em {} (req/rep) e {} (pub)",
        server_id, broker_url, proxy_url
    );

    let mut srv = Server {
        store: load(&store_path),
        store_path,
        messages_path,
        publisher,
    };

    loop {
        let raw = sock.recv_bytes(0).unwrap();
        let reply = handle(&raw, &mut srv);
        sock.send(&reply[..], 0).unwrap();
    }
}
