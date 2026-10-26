use prost::Message;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/chat.rs"));
}

use proto::{reply, request, ChannelList, CreateChannelReply, LoginReply, Reply, Request};

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

fn handle(raw: &[u8], store: &mut Store, path: &str) -> Vec<u8> {
    let body = match Request::decode(raw) {
        Ok(req) => req.body,
        Err(_) => None,
    };

    let reply_body = match body {
        Some(request::Body::Login(login)) => {
            let status = if valid_name(&login.user, 3, 20) {
                store.logins.push(LoginRec {
                    user: login.user.clone(),
                    timestamp: now_ms(),
                });
                save(store, path);
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
                save(store, path);
                println!("canal criado: {} por {}", create.name, create.user);
                "OK"
            };
            reply::Body::CreateChannel(CreateChannelReply {
                status: status.to_string(),
            })
        }

        // Mensagem desconhecida ou vazia: responde com lista vazia e status de erro.
        None => reply::Body::Channels(ChannelList {
            status: "INVALID_REQUEST".to_string(),
            channels: vec![],
        }),
    };

    Reply {
        timestamp: now_ms(),
        body: Some(reply_body),
    }
    .encode_to_vec()
}

fn main() {
    let server_id = env::var("SERVER_ID").unwrap_or_else(|_| "1".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or_else(|_| "tcp://broker:5556".to_string());
    let path = format!("/app/data/server_{}.json", server_id);

    if let Some(dir) = Path::new(&path).parent() {
        fs::create_dir_all(dir).unwrap();
    }

    let mut store = load(&path);

    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REP).unwrap();
    sock.connect(&broker_url).unwrap();

    println!("servidor {} conectado em {}", server_id, broker_url);

    loop {
        let raw = sock.recv_bytes(0).unwrap();
        let reply = handle(&raw, &mut store, &path);
        sock.send(&reply[..], 0).unwrap();
    }
}