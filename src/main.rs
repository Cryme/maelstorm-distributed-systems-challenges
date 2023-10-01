use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::error::Error;
use std::fmt::{Debug, Display};
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
    #[error("Node id mismatch")]
    NodeIdMismatch,
}

struct Node<Input, Output> {
    state: NodeState,
    next_message_id: i32,
    #[cfg(feature = "log_to_file")]
    log_file: File,
    id: Option<String>,
    all_node_ids: Vec<String>,
    broadcast_messages: Vec<i32>,
    topology: Option<serde_json::Value>,
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
            broadcast_messages: Vec::new(),
            topology: None,
            input: Some(input),
            output,
        }
    }

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
        let reply = self.build_reply(message);
        let mut data = serde_json::to_string(&reply).unwrap();

        data.push('\n');

        self.log_to_file(&format!("\n<-- {data}"));
        self.output.write_all(data.as_bytes()).unwrap();
        self.log_to_file(&"\n--");
    }

    fn wrap_err(&self, err: NodeError) -> Payload {
        Payload::Error {
            code: match &err {
                NodeError::UnacceptablePayloadType(..)
                | NodeError::IllegalPayloadType
                | NodeError::NodeIdMismatch => MaelstromError::MalformedRequest,

                NodeError::CurrentlyUnsupported => MaelstromError::NotSupported,
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

            Payload::Broadcast { message } => {
                self.broadcast_messages.push(message);
                Ok(Payload::BroadcastOk)
            }

            Payload::Read => Ok(Payload::ReadOk {
                messages: self.broadcast_messages.clone(),
            }),

            Payload::Topology(v) => {
                self.topology = Some(v);

                Ok(Payload::TopologyOk)
            }

            Payload::EchoOk { .. }
            | Payload::Error { .. }
            | Payload::InitOk
            | Payload::BroadcastOk
            | Payload::ReadOk { .. }
            | Payload::TopologyOk
            | Payload::GenerateOk { .. } => Err(NodeError::IllegalPayloadType),
        }
    }

    fn on_err(&mut self, _error: &dyn Error) {}

    fn build_reply(&mut self, message: Message) -> Message {
        let dst = message.dst.clone();
        let src = message.src.clone();
        let msg_id = message.body.msg_id;

        let payload = match self.proceed_message(message) {
            Ok(payload) => payload,
            Err(e) => {
                self.on_err(&e);

                self.wrap_err(e)
            }
        };

        Message {
            src: if let Some(id) = &self.id {
                id.clone()
            } else {
                dst
            },
            dst: src,
            body: Body {
                msg_id: Some(self.next_message_id()),
                in_reply_to: msg_id,
                payload,
            },
        }
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

    Broadcast {
        message: i32,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<i32>,
    },

    Topology(serde_json::Value),
    TopologyOk,

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
