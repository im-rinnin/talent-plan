use std::{cell::RefCell, fmt, ops::Add, process::id};

use futures::executor::block_on;
use linearizability::models::Op;

use crate::proto::kvraftpb::*;

enum ClientOp {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    last_leader_index: RefCell<Option<usize>>,
    request_id: RefCell<u64>,
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
        Clerk {
            name,
            servers,
            last_leader_index: RefCell::new(None),
            request_id: RefCell::new(0),
        }
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
            client_id: self.name.clone(),
        };
        //  pick node
        let mut index = self.pick_node(None);
        loop {
            let client = self.servers.get(index).unwrap();
            info!("{} send get request {:?} to {}", self.name, args, index);
            let res = block_on(client.get(&args));
            info!("{} send get request reply {:?}", self.name, res);
            match res {
                Ok(reply) => {
                    if reply.success {
                        // update leader node ,return res
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
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: ClientOp) {
        // create message,add unique id
        let mut args = match op {
            ClientOp::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Put as i32,
                client_id: self.name.clone(),
                request_id: *self.request_id.borrow(),
            },
            ClientOp::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: Op::Append as i32,
                client_id: self.name.clone(),
                request_id: *self.request_id.borrow(),
            },
        };
        //  pick node
        let mut index = self.pick_node(None);
        loop {
            let client = self.servers.get(index).unwrap();
            // send command wait until timeout
            // let timeout = chan::after(Duration::from_millis(50));
            info!("{} client send put_append request {:?} ", self.name, args);
            let res = block_on(client.put_append(&args));
            info!(
                "{} client send put_append finish,res is {:?}",
                self.name, res
            );
            match res {
                Ok(reply) => {
                    if reply.success {
                        // update leader node ,return res
                        let mut index_ref = self.last_leader_index.borrow_mut();
                        *index_ref = Some(index);
                        let mut id_ref = self.request_id.borrow_mut();
                        *id_ref += 1;
                        debug!("{} send put append ok", self.name);
                        return;
                    }
                    if reply.wrong_leader {
                        info!("{} send append wrong leader, retry", self.name);
                        index = self.pick_node(Some(index));
                    }
                    // check if request_id is bigger and update
                    else if reply.latest_request_id > args.request_id {
                        args.request_id = reply.latest_request_id;
                        let id_ref = self.request_id.try_borrow_mut();
                        if let Err(e) = id_ref {
                            panic!("error borrow {:?}", e);
                        }
                        let mut id_ref = id_ref.unwrap();
                        *id_ref = reply.latest_request_id;
                        warn!(
                            "{} send append wrong request, update to {} and retry",
                            self.name, *id_ref
                        );
                    }
                }
                // reply error
                Err(e) => {
                    warn!("{} send put_append error {:?},try other node", self.name, e);
                    index = self.pick_node(Some(index));
                }
            }
        }
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
