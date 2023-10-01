use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::error::Error;
use std::fmt::{Debug, Display};
#[cfg(feature = "log_to_file")]
use std::fs::File;
use std::io::{Read, Write};

use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "log_to_file")]
const DEBUG_FILE_PATH: &str = "/home/cryme/RustroverProjects/maelstorm_distrib_challanges/res.txt";

fn main() {
    let std_in = std::io::stdin().lock();
    let std_out = std::io::stdout().lock();

    let node = Node::new(std_in, std_out);

    node.run();
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum NodeState {
    Created,
    Initialized,
}

#[allow(dead_code)]
#[derive(Error, Debug)]
enum NodeError {
    #[error("Unacceptable payload type: {0} for state: {1:#?}")]
    UnacceptablePayloadType(String, NodeState),
    #[error("Support unimplemented type")]
    CurrentlyUnsupported,
    #[error("Bad payload type")]
    IllegalPayloadType,
    #[error("Bad payload")]
    IllegalPayload,
    #[error("Node id mismatch")]
    NodeIdMismatch,
    #[error("")]
    DontReply,
}

struct Node<Input, Output> {
    state: NodeState,
    next_message_id: i32,
    #[cfg(feature = "log_to_file")]
    log_file: File,
    id: Option<String>,
    all_node_ids: Vec<String>,
    #[cfg(feature = "broadcast")]
    broadcast_messages: Vec<i32>,
    #[cfg(feature = "broadcast")]
    heard_rumors: Vec<Uuid>,
    #[cfg(feature = "counter")]
    grow_value: u32,
    neighbours: Vec<String>,
    input: Option<Input>,
    output: Output,
}

impl<Input: Read, Output: Write> Node<Input, Output> {
    fn new(input: Input, output: Output) -> Node<Input, Output> {
        Self {
            state: NodeState::Created,
            next_message_id: i32::MIN,
            #[cfg(feature = "log_to_file")]
            log_file: File::create(DEBUG_FILE_PATH).unwrap(),
            id: None,
            all_node_ids: Vec::new(),
            #[cfg(feature = "broadcast")]
            broadcast_messages: Vec::new(),
            #[cfg(feature = "broadcast")]
            heard_rumors: Vec::new(),
            #[cfg(feature = "counter")]
            grow_value: 0,
            #[cfg(feature = "broadcast")]
            neighbours: Vec::new(),
            input: Some(input),
            output,
        }
    }

    #[allow(unused_variables)]
    fn log_to_file(&mut self, data: &dyn Display) {
        #[cfg(feature = "log_to_file")]
        writeln!(self.log_file, "{data}").unwrap();
    }

    fn next_message_id(&mut self) -> i32 {
        self.next_message_id += 1;

        self.next_message_id
    }

    fn run(mut self) {
        if self.input.is_none() {
            return;
        }

        self.log_to_file(&"Created!");

        let input = self.input.take().unwrap();

        let msg = serde_json::Deserializer::from_reader(input).into_iter::<Message>();

        for m in msg {
            self.log_to_file(&format!("\n--> {m:#?}"));

            let Ok(message) = m else { continue };

            self.handle_message(message);
        }
    }

    fn handle_message(&mut self, message: Message) {
        if let Some(reply) = self.build_reply(message) {
            self.send_to_network(&reply);
        }
    }

    fn send_to_network<T: Sized + Serialize>(&mut self, data: &T) {
        let mut data = serde_json::to_string(data).unwrap();

        data.push('\n');

        self.log_to_file(&format!("\n<-- {data}"));
        self.output.write_all(data.as_bytes()).unwrap();
        self.log_to_file(&"\n--");
    }

    fn spread(&mut self, payload: Payload) {
        let Some(id) = self.id.clone() else {
            return;
        };

        for neighbour in self.neighbours.clone() {
            let message = self.wrap_payload(payload.clone(), id.clone(), neighbour, None);

            self.send_to_network(&message);
        }
    }

    fn wrap_err(&self, err: NodeError) -> Payload {
        Payload::Error {
            code: match &err {
                NodeError::UnacceptablePayloadType(..)
                | NodeError::IllegalPayloadType
                | NodeError::IllegalPayload
                | NodeError::NodeIdMismatch => MaelstromError::MalformedRequest,

                NodeError::CurrentlyUnsupported => MaelstromError::NotSupported,

                NodeError::DontReply => unreachable!(),
            },
            text: format!("{err:#?}"),
        }
    }

    fn proceed_message(&mut self, message: Message) -> Result<Payload, NodeError> {
        if let Some(id) = &self.id {
            if id != &message.dst {
                return Err(NodeError::NodeIdMismatch);
            }
        }

        match message.body.payload {
            Payload::Init { node_id, node_ids } => {
                if self.state != NodeState::Created {
                    return Err(NodeError::UnacceptablePayloadType(
                        "Init".to_string(),
                        self.state,
                    ));
                }

                self.id = Some(node_id);
                self.all_node_ids = node_ids;
                self.state = NodeState::Initialized;

                Ok(Payload::InitOk)
            }

            Payload::Echo { echo } => {
                if self.state != NodeState::Initialized {
                    return Err(NodeError::UnacceptablePayloadType(
                        "Echo".to_string(),
                        self.state,
                    ));
                }

                Ok(Payload::EchoOk { echo })
            }

            Payload::Generate => Ok(Payload::GenerateOk { id: Uuid::new_v4() }),

            #[cfg(feature = "broadcast")]
            Payload::Broadcast { message } => {
                self.broadcast_messages.push(message);

                let rumor_id = Uuid::new_v4();
                self.heard_rumors.push(rumor_id);

                let rumor = Payload::Rumor {
                    id: rumor_id,
                    data: message,
                };

                self.spread(rumor);

                Ok(Payload::BroadcastOk)
            }

            #[cfg(feature = "counter")]
            Payload::Add { delta } => {
                self.grow_value += delta;

                Ok(Payload::AddOk)
            }

            Payload::Read => Ok(Payload::ReadOk {
                #[cfg(feature = "broadcast")]
                messages: self.broadcast_messages.clone(),
                #[cfg(feature = "counter")]
                value: self.grow_value,
            }),

            #[cfg(feature = "broadcast")]
            Payload::Topology { topology } => {
                let Some(id) = self.id.clone() else {
                    return Err(NodeError::UnacceptablePayloadType(
                        "Topology".to_string(),
                        self.state,
                    ));
                };

                let Some(val) = topology.as_object() else {
                    return Err(NodeError::IllegalPayload);
                };

                let Some(val) = val.get(&id) else {
                    return Err(NodeError::IllegalPayload);
                };

                let Ok(top) = serde_json::from_value(val.clone()) else {
                    return Err(NodeError::IllegalPayload);
                };

                self.neighbours = top;

                self.log_to_file(&format!("node {id} -: {:#?}", self.neighbours));

                Ok(Payload::TopologyOk)
            }

            #[cfg(feature = "broadcast")]
            Payload::Rumor { id, data } => {
                if !self.heard_rumors.contains(&id) {
                    self.broadcast_messages.push(data);
                    self.heard_rumors.push(id);

                    self.spread(Payload::Rumor { id, data });
                }

                Ok(Payload::RumorOk)
            }

            Payload::Error { .. } => Err(NodeError::IllegalPayloadType),

            Payload::EchoOk { .. }
            | Payload::InitOk
            | Payload::BroadcastOk
            | Payload::ReadOk { .. }
            | Payload::TopologyOk
            | Payload::AddOk
            | Payload::RumorOk
            | Payload::GenerateOk { .. } => Err(NodeError::DontReply),
        }
    }

    fn on_err(&mut self, _error: &dyn Error) {}

    fn wrap_payload(
        &mut self,
        payload: Payload,
        src: String,
        dst: String,
        msg_id: Option<i32>,
    ) -> Message {
        Message {
            src: if let Some(id) = &self.id {
                id.clone()
            } else {
                src
            },
            dst,
            body: Body {
                msg_id: Some(self.next_message_id()),
                in_reply_to: msg_id,
                payload,
            },
        }
    }

    fn build_reply(&mut self, message: Message) -> Option<Message> {
        let dst = message.dst.clone();
        let src = message.src.clone();
        let msg_id = message.body.msg_id;

        let payload = match self.proceed_message(message) {
            Ok(payload) => payload,
            Err(e) => {
                if let NodeError::DontReply = e {
                    return None;
                }

                self.on_err(&e);

                self.wrap_err(e)
            }
        };

        Some(self.wrap_payload(payload, dst, src, msg_id))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Body {
    msg_id: Option<i32>,
    in_reply_to: Option<i32>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,

    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    Generate,
    GenerateOk {
        id: Uuid,
    },

    #[cfg(feature = "broadcast")]
    Broadcast {
        message: i32,
    },
    BroadcastOk,

    #[cfg(feature = "counter")]
    Add {
        delta: u32,
    },
    AddOk,

    Read,
    ReadOk {
        #[cfg(feature = "broadcast")]
        messages: Vec<i32>,
        #[cfg(feature = "counter")]
        value: u32,
    },

    #[cfg(feature = "broadcast")]
    Topology {
        topology: serde_json::Value,
    },
    TopologyOk,

    #[cfg(feature = "broadcast")]
    Rumor {
        id: Uuid,
        data: i32,
    },
    RumorOk,

    Error {
        code: MaelstromError,
        text: String,
    },
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
#[repr(u8)]
enum MaelstromError {
    /**
        Indicates that the requested operation could not be completed within a Timeout.
    */
    Timeout = 0,
    /**
        Thrown when a client sends an RPC request to a node which does not exist.
    */
    NodeNotFound = 1,
    /**
        Use this error to indicate that a requested operation is not supported by the current implementation. Helpful for stubbing out APIs during development.
    */
    NotSupported = 10,
    /**
    Indicates that the operation definitely cannot be performed at this time--perhaps because the server is in a read-only state, has not yet been initialized, believes its peers to be down, and so on. Do not use this error for indeterminate cases, when the operation may actually have taken place.
    */
    TemporarilyUnavailable = 11,
    /**
        The client's request did not conform to the server's expectations, and could not possibly have been processed.
    */
    MalformedRequest = 12,
    /**
        Indicates that some kind of general, indefinite error occurred. Use this as a catch-all for errors you can't otherwise categorize, or as a starting point for your error handler: it's safe to return internal-error for every problem by default, then add special cases for more specific errors later.
    */
    Crash = 13,
    /**
        Indicates that some kind of general, definite error occurred. Use this as a catch-all for errors you can't otherwise categorize, when you specifically know that the requested operation has not taken place. For instance, you might encounter an indefinite failure during the prepare phase of a transaction: since you haven't started the commit process yet, the transaction can't have taken place. It's therefore safe to return a definite abort to the client.
    */
    Abort = 14,
    /**
        The client requested an operation on a key which does not exist (assuming the operation should not automatically create missing keys).
    */
    KeyDoesNotExist = 20,
    /**
        The client requested the creation of a key which already exists, and the server will not overwrite it.
    */
    KeyAlreadyExists = 21,
    /**
        The requested operation expected some conditions to hold, and those conditions were not met. For instance, a compare-and-set operation might assert that the value of a key is currently 5; if the value is 3, the server would return precondition-failed.
    */
    PreconditionFailed = 22,
    /**
        The requested transaction has been aborted because of a conflict with another transaction. Servers need not return this error on every conflict: they may choose to retry automatically instead.
    */
    TxnConflict = 30,
}
