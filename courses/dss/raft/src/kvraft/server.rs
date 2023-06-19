use std::collections::HashMap;
use std::thread::spawn;
use std::{future, todo};

use crossbeam_channel::{Receiver, Sender};
use futures::channel::mpsc::unbounded;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::StreamExt;

use crate::proto::kvraftpb::*;
use crate::raft;
#[derive(Debug)]

enum KvServerEvent {
    PutAppend {
        client_id: String,
        request_id: u64,
        key: String,
        value: String,
        op: Op,
        reply: oneshot::Sender<PutAppendReply>,
    },
    Get {
        client_id: String,
        key: String,
        reply: oneshot::Sender<GetReply>,
    },
    Stop(),
    GetState {
        reply: oneshot::Sender<raft::State>,
    },
    DataChange(StateMachineDataChange),
    InstallSnapshot(StateMachine),
    ReadData(ReadData),
}

// (client_id,request_id)
type ReplyId = (String, u64);

pub struct KvServer {
    pub rf: raft::Node,
    state_machine: StateMachine,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    rx: Receiver<KvServerEvent>,
    tx: Sender<KvServerEvent>,
    get_replys: HashMap<String, oneshot::Sender<GetReply>>,
    put_append_replys: HashMap<ReplyId, oneshot::Sender<PutAppendReply>>, // Your definitions here.
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let res = apply_ch.for_each(handle_raft_apply(event_tx.clone()));

        spawn(|| block_on(res));
        let state_machine = StateMachine {
            kv_map: HashMap::new(),
            client_request_id: HashMap::new(),
        };

        let node = raft::Node::new(rf);

        KvServer {
            rf: node,
            state_machine,
            me,
            maxraftstate,
            rx: event_rx,
            tx: event_tx,
            get_replys: HashMap::new(),
            put_append_replys: HashMap::new(),
        }

        // crate::your_code_here((rf, maxraftstate, apply_ch))
    }
    fn run(self) -> Sender<KvServerEvent> {
        let tx = self.tx.clone();
        spawn(move || self.main());
        tx
    }

    fn main(mut self) {
        info!("{} start event loop", self.me);
        loop {
            let event_res = self.rx.recv();
            match event_res {
                Err(e) => {
                    info!("{} recv event error {:?},return loop", self.me, e);
                    return;
                }
                Ok(event) => {
                    info!("{} receive event {:?}", self.me, event);
                    match event {
                        KvServerEvent::PutAppend {
                            client_id,
                            request_id,
                            key,
                            value,
                            reply,
                            op,
                        } => self.handle_put_append_request(
                            client_id, request_id, op, key, value, reply,
                        ),
                        KvServerEvent::Get {
                            reply,
                            client_id,
                            key,
                        } => self.handle_get_request(client_id, key, reply),
                        KvServerEvent::GetState { reply } => self.handle_get_state(reply),
                        KvServerEvent::Stop() => {
                            self.rf.kill();
                            info!("{} receve stop, return", self.me);
                            return;
                        }
                        KvServerEvent::DataChange(data_change) => {
                            self.handle_put_append_apply(data_change)
                        }
                        KvServerEvent::InstallSnapshot(_) => todo!(),
                        KvServerEvent::ReadData(ReadData { key, client_id }) => {
                            self.handle_get_reply(client_id, key)
                        }
                    }
                }
            }
            debug!("{} end loop", self.me);
        }
    }
    fn handle_put_append_request(
        &mut self,
        client_id: String,
        request_id: u64,
        op: Op,
        key: String,
        value: String,
        reply: oneshot::Sender<PutAppendReply>,
    ) {
        // refuse if not leader
        if !self.rf.is_leader() {
            let res = reply.send(PutAppendReply {
                wrong_leader: true,
                err: String::from("not leader"),
                next_request_id: 0,
                success: false,
                id_not_match: false,
            });
            if let Err(e) = res {
                warn!("{} send put append reply error {:?}", self.me, e)
            }
            return;
        }
        // send data change to raft node
        let put_append_request = PutAppendRequest {
            key,
            value,
            op: op as i32,
            client_id: client_id.clone(),
            request_id,
        };
        let mut data = vec![];
        labcodec::encode(&put_append_request, &mut data).unwrap();
        let command = RaftCommand {
            data,
            command_type: RaftCommandType::PutAppend as i32,
        };
        let res = self.rf.start(&command);
        if let Err(e) = res {
            warn!("{} send put append requset to node error {:?}", self.me, e);

            reply
                .send(PutAppendReply {
                    wrong_leader: false,
                    err: e.to_string(),
                    next_request_id: *self
                        .state_machine
                        .client_request_id
                        .get(&client_id)
                        .unwrap_or(&0),
                    success: false,
                    id_not_match: false,
                })
                .unwrap();
            return;
        }
        debug!("{} save put reply {} ", self.me, client_id);
        // save reply
        self.put_append_replys
            .insert((client_id, request_id), reply);
    }
    fn handle_get_request(
        &mut self,
        client_id: String,
        key: String,
        reply: oneshot::Sender<GetReply>,
    ) {
        // refuse if not leader
        if !self.rf.is_leader() {
            reply
                .send(GetReply {
                    wrong_leader: true,
                    err: String::from("not leader"),
                    value: String::new(),
                    success: false,
                })
                .unwrap();
            return;
        }
        // send read data to raft node
        let request = ReadData {
            key,
            client_id: client_id.clone(),
        };
        let mut data = vec![];
        labcodec::encode(&request, &mut data).unwrap();

        let command = RaftCommand {
            command_type: RaftCommandType::Read as i32,
            data,
        };

        debug!(
            "{} handle get request, send command {:?}to raft",
            self.me, command
        );
        let res = self.rf.start(&command);
        if let Err(e) = res {
            warn!("{} send read data to raft error{:?}", self.me, e);
            reply
                .send(GetReply {
                    wrong_leader: false,
                    err: e.to_string(),
                    value: String::new(),
                    success: false,
                })
                .unwrap();
            return;
        }

        // save reply
        debug!("{} save get reply {} ", self.me, client_id);
        self.get_replys.insert(client_id, reply);
    }
    fn handle_get_state(&mut self, reply: oneshot::Sender<raft::State>) {
        let rx = self.rf.get_state_ch();
        spawn(move || {
            let res = rx.recv();
            if let Ok(s) = res {
                let res = reply.send(s);
                if let Err(e) = res {
                    warn!("send get state result error {:?}", e);
                }
            }
            // drop reply if err
        });
    }
    fn handle_put_append_apply(&mut self, data_change: StateMachineDataChange) {
        debug!("{} handle put append apply {:?}", self.me, data_change);
        let reply_res = self
            .put_append_replys
            .remove(&(data_change.client_id.clone(), data_change.request_id));
        if reply_res.is_none() {
            info!(
                "{} put_append reply {} {} not found,just ignore",
                self.me, data_change.client_id, data_change.request_id
            );
            return;
        }

        // check request id
        let id = self
            .state_machine
            .client_request_id
            .entry(data_change.client_id.clone())
            .or_insert(0);
        if data_change.request_id != *id {
            info!(
                "{} receive data change, requset is {},current id is {},not match reject it",
                self.me, data_change.request_id, *id
            );
            reply_res
                .map(|reply| {
                    let _ = reply.send(PutAppendReply {
                        wrong_leader: false,
                        err: String::from("request id not match"),
                        success: false,
                        next_request_id: *id,
                        id_not_match: true,
                    });
                })
                .unwrap();
            return;
        }
        // apply data to state machine
        match data_change.op {
            // put
            1 => {
                self.state_machine
                    .kv_map
                    .insert(data_change.key, data_change.value);
            }
            // append
            2 => {
                let v = self
                    .state_machine
                    .kv_map
                    .entry(data_change.key)
                    .or_insert(String::from(""));
                v.push_str(&data_change.value);
            }
            a => {
                error!("op {} not match", a)
            }
        }
        // reply
        let res = reply_res.map(|reply| {
            reply.send(PutAppendReply {
                wrong_leader: false,
                err: String::new(),
                success: true,
                next_request_id: *id,
                id_not_match: false,
            })
        });
        // update request id
        *id += 1;
        if let Some(Err(e)) = res {
            warn!("{} send put append reply error {:?}", self.me, e);
        }
    }

    fn handle_get_reply(&mut self, client_id: String, key: String) {
        // get reply ch
        debug!(
            "{} start handle get reply for {} {}",
            self.me, client_id, key
        );

        let reply_res = self.get_replys.remove(&client_id);
        if let None = reply_res {
            warn!("{} get reply for {} not found", self.me, client_id);
            return;
        }
        let reply = reply_res.unwrap();

        // get data
        let default_value = String::new();
        let res = self
            .state_machine
            .kv_map
            .get(&key)
            .unwrap_or(&default_value);
        //  send to reply
        let send_res = reply.send(GetReply {
            wrong_leader: false,
            err: String::new(),
            value: res.clone(),
            success: true,
        });
        if let Err(e) = send_res {
            error!(
                "{} send get reply for {}  error {:?}",
                self.me, client_id, e
            );
        }
    }

    fn handle_install_snapshot(&mut self, reply: oneshot::Sender<raft::State>) {
        todo!()
    }
}

fn handle_raft_apply(
    event_tx_clone: Sender<KvServerEvent>,
) -> impl Fn(raft::ApplyMsg) -> future::Ready<()> {
    debug!("handle apply start");
    move |msg| {
        debug!("handle_raft_apply receive raft apply {:?}", msg);
        match msg {
            raft::ApplyMsg::Command { data, index: _ } => {
                let command = labcodec::decode::<RaftCommand>(&data).unwrap();
                debug!("handle read data reply");
                match RaftCommandType::from_i32(command.command_type).unwrap() {
                    RaftCommandType::Read => {
                        let read = labcodec::decode::<ReadData>(&command.data).unwrap();
                        let res = event_tx_clone.send(KvServerEvent::ReadData(read));
                        if let Err(e) = res {
                            warn!("send read data error {:?}", e);
                        }
                    }
                    RaftCommandType::PutAppend => {
                        debug!("handle put append reply");
                        let put_append_request =
                            labcodec::decode::<PutAppendRequest>(&command.data);
                        match put_append_request {
                            Ok(data_change) => {
                                let event = KvServerEvent::DataChange(StateMachineDataChange {
                                    key: data_change.key,
                                    value: data_change.value,
                                    client_id: data_change.client_id,
                                    request_id: data_change.request_id,
                                    op: data_change.op,
                                });
                                let res = event_tx_clone.send(event);
                                if let Err(e) = res {
                                    warn!("send data change error {:?}", e);
                                }
                                debug!("handle put append reply success");
                            }
                            Err(e) => {
                                error!("decode error {:?}", e);
                            }
                        }
                    }
                }
                future::ready(())
            }
            raft::ApplyMsg::Snapshot {
                data,
                term: _,
                index: _,
            } => {
                let state_machine = labcodec::decode::<StateMachine>(&data).unwrap();
                let res = event_tx_clone.send(KvServerEvent::InstallSnapshot(state_machine));
                if let Err(e) = res {
                    warn!("send install state machine error {:?}", e);
                }
                future::ready(())
            }
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    tx: Sender<KvServerEvent>, // Your definitions here.
    id: usize,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let id = kv.me;
        let tx = kv.run();
        Node { tx, id }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.

        let res = self.tx.send(KvServerEvent::Stop());
        if let Err(e) = res {
            warn!("{} send stop error {:?}", self.id, e);
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        let (tx, rx) = oneshot::channel();
        let res = self.tx.send(KvServerEvent::GetState { reply: tx });
        if let Err(e) = res {
            error!("{} send get state event error {:?}", self.id, e);
            panic!("error");
        }
        let read_res = block_on(rx);
        match read_res {
            Ok(state) => state,
            Err(e) => {
                panic!("{} read state erro {:?}", self.id, e);
            }
        }
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        let (tx, rx) = oneshot::channel();
        let event = KvServerEvent::Get {
            client_id: arg.client_id,
            key: arg.key,
            reply: tx,
        };

        let res = self.tx.send(event);
        if let Err(e) = res {
            warn!("{} send get event to node error {:?}", self.id, e);
            return Err(labrpc::Error::Other(String::from("send error:")));
        }
        let res = rx.await;
        match res {
            Ok(res) => Ok(res),
            Err(e) => {
                warn!("{} send get but canceled {:?}", self.id, e);
                Err(labrpc::Error::Other(String::from("send error:")))
            }
        }
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        let (tx, rx) = oneshot::channel();
        let event = KvServerEvent::PutAppend {
            client_id: arg.client_id,
            request_id: arg.request_id,
            op: Op::from_i32(arg.op).unwrap(),
            key: arg.key,
            value: arg.value,
            reply: tx,
        };

        let res = self.tx.send(event);
        if let Err(e) = res {
            warn!("{} send put_append event to node error {:?}", self.id, e);
            return Err(labrpc::Error::Other(String::from("send error:")));
        }
        let res = rx.await;
        match res {
            Ok(res) => Ok(res),
            Err(e) => {
                warn!("{} send put but canceled {:?}", self.id, e);
                Err(labrpc::Error::Other(String::from("send put append error:")))
            }
        }
    }
}
