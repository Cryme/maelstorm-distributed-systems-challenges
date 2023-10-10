use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

pub struct Storage {
    map: DashMap<String, Vec<usize>>,
}

#[derive(Serialize, Deserialize)]
pub enum ClientPacket {
    Hello,
    Store { key: String, msg: usize },
    Get { key: String, offset: usize },
}

#[derive(Serialize, Deserialize)]
pub enum StoragePacket {
    Hello,
    Store(usize),
    Get(Vec<usize>),
}

impl Storage {
    pub(crate) fn run() {
        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();

            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:14081").await.unwrap();

                let storage = Arc::new(Storage {
                    map: Default::default(),
                });

                loop {
                    let (stream, _) = listener.accept().await.unwrap();

                    let storage = storage.clone();

                    tokio::spawn(async move {
                        let (mut read, mut write) = stream.into_split();

                        let mut data_in = [0u8; 1024];

                        while let Ok(count) = read.read(&mut data_in).await {
                            if count == 0 {
                                break;
                            }

                            let Ok(packet) =
                                bincode::deserialize::<ClientPacket>(&data_in[..count])
                            else {
                                continue;
                            };

                            let packet = match packet {
                                ClientPacket::Hello => StoragePacket::Hello,

                                ClientPacket::Store { key, msg } => {
                                    let offset = if let Some(mut v) = storage.map.get_mut(&key) {
                                        (*v).push(msg);

                                        v.len() - 1
                                    } else {
                                        storage.map.insert(key, vec![msg]);

                                        0
                                    };

                                    StoragePacket::Store(offset)
                                }

                                ClientPacket::Get { key, offset } => {
                                    let mut res = Vec::new();

                                    if let Some(v) = storage.map.get(&key) {
                                        if v.len() > offset {
                                            res.extend_from_slice(&v[offset..])
                                        }
                                    }

                                    StoragePacket::Get(res)
                                }
                            };

                            let _ = write
                                .write(&bincode::serialize::<StoragePacket>(&packet).unwrap())
                                .await;
                        }
                    });
                }
            });
        });
    }
}

#[cfg(test)]
mod tests {
}
