use prost::Message;
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/chat.rs"));
}

use proto::{
    ref_reply, ref_request, HeartbeatReply, RefReply, RefRequest, RegisterReply, ServerInfo,
    ServerList,
};

// Um servidor cadastrado: rank fixo e momento do último sinal de vida.
struct Entry {
    rank: u32,
    last_seen: Instant,
}

// Estado do serviço de referência.
struct Registry {
    servers: HashMap<String, Entry>,
    next_rank: u32,
    timeout: Duration,
    clock: u64, // relógio lógico da referência
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl Registry {
    // Remove servidores que não mandaram heartbeat dentro do prazo.
    fn purge(&mut self) {
        let timeout = self.timeout;
        self.servers.retain(|nome, entry| {
            let ativo = entry.last_seen.elapsed() <= timeout;
            if !ativo {
                println!("removido da lista: {} (sem heartbeat)", nome);
            }
            ativo
        });
    }
}

// Regra do relógio lógico: usa o maior valor entre o seu e o recebido, e depois incrementa.
fn ao_receber(reg: &mut Registry, recebido: u64) {
    reg.clock = reg.clock.max(recebido) + 1;
}

fn handle(raw: &[u8], reg: &mut Registry) -> Vec<u8> {
    let req = match RefRequest::decode(raw) {
        Ok(r) => r,
        Err(_) => {
            println!("mensagem inválida ({} bytes)", raw.len());
            return Vec::new();
        }
    };
    ao_receber(reg, req.clock);
    println!("RECEBIDO  {:?}", req);

    reg.purge();

    let body = match req.body {
        Some(ref_request::Body::Register(r)) => {
            let rank = match reg.servers.get_mut(&r.name) {
                // Já cadastrado: não repete o nome, devolve o rank que ele já tinha
                Some(entry) => {
                    entry.last_seen = Instant::now();
                    entry.rank
                }
                None => {
                    let rank = reg.next_rank;
                    reg.next_rank += 1;
                    reg.servers.insert(
                        r.name.clone(),
                        Entry {
                            rank,
                            last_seen: Instant::now(),
                        },
                    );
                    println!("cadastrado: {} com rank {}", r.name, rank);
                    rank
                }
            };
            Some(ref_reply::Body::Register(RegisterReply { rank }))
        }

        Some(ref_request::Body::List(_)) => {
            let mut servers: Vec<ServerInfo> = reg
                .servers
                .iter()
                .map(|(nome, entry)| ServerInfo {
                    name: nome.clone(),
                    rank: entry.rank,
                })
                .collect();
            servers.sort_by_key(|s| s.rank);
            Some(ref_reply::Body::List(ServerList { servers }))
        }

        Some(ref_request::Body::Heartbeat(h)) => match reg.servers.get_mut(&h.name) {
            Some(entry) => {
                entry.last_seen = Instant::now();
                Some(ref_reply::Body::Heartbeat(HeartbeatReply {
                    status: "OK".to_string(),
                    time_ms: now_ms(),
                }))
            }
            None => Some(ref_reply::Body::Heartbeat(HeartbeatReply {
                status: "NAO_CADASTRADO".to_string(),
                time_ms: now_ms(),
            })),
        },

        None => None,
    };

    reg.clock += 1;
    let reply = RefReply {
        timestamp: now_ms(),
        clock: reg.clock,
        body,
    };
    println!("ENVIADO   {:?}", reply);
    reply.encode_to_vec()
}

fn main() {
    let url = env::var("REF_URL").unwrap_or_else(|_| "tcp://*:5559".to_string());
    // Tempo sem heartbeat até o servidor ser removido da lista (em segundos)
    let timeout_s: u64 = env::var("HEARTBEAT_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let mut reg = Registry {
        servers: HashMap::new(),
        next_rank: 1,
        timeout: Duration::from_secs(timeout_s),
        clock: 0,
    };

    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REP).unwrap();
    sock.bind(&url).unwrap();

    println!(
        "referência em {} (heartbeat limite: {}s)",
        url, timeout_s
    );

    loop {
        let raw = sock.recv_bytes(0).unwrap();
        let reply = handle(&raw, &mut reg);
        sock.send(&reply[..], 0).unwrap();
    }
}