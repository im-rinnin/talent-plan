use std::{
    cell::RefCell,
    fmt,
    sync::mpsc::channel,
    thread::spawn,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::executor::block_on;
use linearizability::models::Op;

use crate::proto::kvraftpb::*;

enum ClientOp {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub id: String,
    pub servers: Vec<KvClient>,
    last_leader_index: RefCell<Option<usize>>,
    request_id: RefCell<u64>,
}

fn system_time_now_epoch() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_nanos() as u64
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        let time = Self::system_time_now_epoch().to_string();
        let rand = rand::random::<u128>();
        let id = format!("{}_{}_{}", name, time, rand);

        Clerk {
            name,
            id,
            servers,
            last_leader_index: RefCell::new(None),
            request_id: RefCell::new(0),
        }
    }

    fn system_time_now_epoch() -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        since_the_epoch.as_nanos() as u64
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = GetRequest {
            key,
            client_id: self.id.clone(),
            timestamp: system_time_now_epoch(),
        };
        //  pick node
        let mut index = self.pick_node(None);
        loop {
            let client = self.servers.get(index).unwrap();
            let c = client.clone();
            info!("{} send get request {:?} to {}", self.name, args, index);

            let (tx, rx) = channel();
            let arg_clone = args.clone();
            spawn(move || {
                let res = block_on(c.get(&arg_clone));
                let _ = tx.send(res);
            });
            // let res = block_on(client.get(&args));
            let timeout_res = rx.recv_timeout(Duration::from_millis(100));
            match timeout_res {
                Ok(res) => {
                    match res {
                        Ok(reply) => {
                            if reply.success {
                                // update leader node ,return res
                                debug!(
                                    "send get request to {} ,key {} value{} request{:?},reply{:?}",
                                    index, args.key, reply.value, args, reply
                                );
                                if args.timestamp != reply.timestamp {
                                    info!("get reply timestamp not match request,retry");
                                    continue;
                                }
                                let mut index_ref = self.last_leader_index.borrow_mut();
                                *index_ref = Some(index);
                                return reply.value;
                            }
                            if reply.wrong_leader {
                                info!("{} send append wrong leader, retry", self.name);
                                index = self.pick_node(Some(index));
                            }
                        }
                        Err(e) => {
                            warn!("{} send get error {:?},try other node", self.name, e);
                            index = self.pick_node(Some(index));
                        }
                    }
                }
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        info!("{} get timeout,just retry", self.name);
                        continue;
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        error!("{} get channel disconned,return", self.name);
                        panic!("error");
                    }
                },
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: ClientOp) {
        // create message,add unique id
        let args = match op {
            ClientOp::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Put as i32,
                client_id: self.id.clone(),
                request_id: *self.request_id.borrow(),
            },
            ClientOp::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Append as i32,
                client_id: self.id.clone(),
                request_id: *self.request_id.borrow(),
            },
        };
        //  pick node
        let mut index = self.pick_node(None);
        loop {
            let client = self.servers.get(index).unwrap();
            let c = client.clone();
            let args_clone = args.clone();
            // send command wait until timeout
            debug!("{} client send put_append request {:?} ", self.name, args);
            let (tx, rx) = channel();
            spawn(move || {
                let res = block_on(c.put_append(&args_clone));
                let _ = tx.send(res);
            });
            let timeout_res = rx.recv_timeout(Duration::from_millis(100));
            match timeout_res {
                Ok(res) => {
                    match res {
                        Ok(reply) => {
                            if reply.success {
                                // update leader node ,return res
                                self.update_request_id(&index);
                                debug!("{} send put append ok", self.name);
                                return;
                            }
                            if reply.wrong_leader {
                                let old_index = index;
                                index = self.pick_node(Some(index));
                                info!(
                                    "{} send append wrong leader {}, retry {}",
                                    self.name, old_index, index
                                );
                            }
                            // check if request_id is bigger and update
                            else if reply.id_not_match {
                                info!(
                                    "{} put append alreay accept,args {},reply {}",
                                    self.name, args.request_id, reply.next_request_id
                                );
                                assert_eq!(
                                    args.request_id + 1,
                                    reply.next_request_id,
                                    "requset id{}, reply id{}",
                                    args.request_id,
                                    reply.next_request_id
                                );
                                self.update_request_id(&index);
                                return;
                            }
                        }
                        // reply error
                        Err(e) => {
                            warn!("{} send put_append error {:?},try other node", self.name, e);
                            index = self.pick_node(Some(index));
                        }
                    }
                }
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        info!("{} get append timeout,just retry", self.name);
                        continue;
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        error!("{} get append channel disconneted,return,", self.name);
                        panic!("error");
                    }
                },
            }
        }
    }

    fn update_request_id(&self, index: &usize) {
        let mut index_ref = self.last_leader_index.borrow_mut();
        *index_ref = Some(*index);
        let mut id_ref = self.request_id.borrow_mut();
        *id_ref += 1;
    }

    // pick last leader node ,if not found ,use random one
    fn pick_node(&self, not_leader: Option<usize>) -> usize {
        let mut l = self.last_leader_index.borrow_mut();
        match l.take() {
            Some(index) => index,
            None => {
                let mut index = rand::random::<usize>() % self.servers.len();
                if let Some(i) = not_leader {
                    if index == i {
                        index = (index + 1) % self.servers.len()
                    }
                }
                info!("{} pick node {} ", self.name, index);
                index
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(ClientOp::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(ClientOp::Append(key, value))
    }
}
