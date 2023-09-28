use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fs::File;
use std::io::Write;

fn main() {
    let mut file =
        File::create("/home/cryme/RustroverProjects/maelstorm_distrib_challanges/res.txt").unwrap();
    writeln!(file, "Created!").unwrap();
    let std_in = std::io::stdin().lock();
    let mut std_out = std::io::stdout().lock();

    let msg = serde_json::Deserializer::from_reader(std_in).into_iter::<Message>();

    for m in msg {
        writeln!(file, "\n--> {m:#?}").unwrap();
        let mut rep = serde_json::to_string(&m.unwrap().into_reply()).unwrap();
        rep.push_str("\n");
        writeln!(file, "\n<-- {rep}").unwrap();
        std_out.write(rep.as_bytes()).unwrap();
        writeln!(file, "\n--").unwrap();
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

impl Message {
    fn into_reply(self) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                msg_id: None,
                in_reply_to: self.body.msg_id,
                payload: match self.body.payload {
                    Payload::Init { .. } => Payload::InitOk,
                    Payload::Echo { echo } => Payload::EchoOk { echo },

                    _ => {
                        unimplemented!()
                    }
                },
            },
        }
    }
}

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
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
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
