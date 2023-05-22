use core::panic;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::channel::mpsc::unbounded;
use futures::channel::oneshot;
use futures::channel::oneshot::channel;
use futures::executor::block_on;
use futures::inner_macro::select;
use log::debug;
use log::error;
use log::info;
use prost::Message;
use std::collections::HashMap;
use std::collections::HashSet;
use std::default;
use std::f32::consts::E;
use std::ops::Add;
use std::sync::Arc;
use std::thread;
use std::thread::spawn;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::todo;
use std::vec;

use futures::channel::mpsc::UnboundedSender;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::kvraftpb::Op;
use crate::proto::raftpb::*;

const HEARTBEAT_CHECK_INTERVAL: u64 = 20;
const ELECTION_TIMEOUT_MIN_TIME: u64 = 150;
const ELECTION_TIMEOUT_RANGE: u64 = 150;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
pub enum Role {
    LEADER = 0,
    FOLLOWER = 1,
    CANDIDATOR = 2,
}

#[derive(Clone, Message)]
pub struct Entry {
    #[prost(bytes, tag = "1")]
    data: Vec<u8>,
    #[prost(uint64, tag = "2")]
    term: u64,
}
#[derive(Clone, prost::Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    pub logs: Vec<Entry>,
    // volatile state
}

#[derive(Clone, Debug)]
pub struct VolatileState {
    role: Role,
    commit_index: u64,
    last_applied: u64,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
    vote_record: HashSet<u64>,
    last_send_append_time: u64,
    // update to current_time+random_time[150,300)
    // when to update:
    // init
    // receive any messages from valid leader/candidate
    time_to_check_election_time_out: u64,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            role: Role::FOLLOWER,
            commit_index: Default::default(),
            last_applied: Default::default(),
            next_index: Default::default(),
            match_index: Default::default(),
            time_to_check_election_time_out: Default::default(),
            last_send_append_time: 0,
            vote_record: Default::default(),
        }
    }
}

/// State of a raft peer.
#[derive(Clone)]
pub struct State {
    p_state: PersistentState,
    v_state: VolatileState,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.p_state.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.v_state.role == Role::LEADER
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    volatile_state: VolatileState,
    persistent_state: PersistentState,
    // other state
    rx: Receiver<RaftEvent>,
    tx: Sender<RaftEvent>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let (tx, rx) = crossbeam_channel::unbounded::<RaftEvent>();

        let mut p = PersistentState::default();
        p.logs.push(Entry {
            data: vec![],
            term: 0,
        });

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            persistent_state: p,
            volatile_state: VolatileState::default(),
            rx: rx,
            tx: tx,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }
    fn random_timeout() -> Duration {
        Duration::from_millis(
            ELECTION_TIMEOUT_MIN_TIME + rand::random::<u64>() % ELECTION_TIMEOUT_RANGE,
        )
    }
    pub fn update_election_check_time_out(&mut self) {
        let start = system_time_now_epoch();
        let check_time = start + Self::random_timeout().as_millis() as u64;
        self.volatile_state.time_to_check_election_time_out = check_time;
    }

    fn main(mut self) {
        loop {
            //
            let res = self.rx.recv();
            info!("receive raft event {:?}", res);
            match res {
                Err(RecvError) => {
                    info!("raft handler chan receive error {},stop handle", RecvError);
                    return;
                }
                Ok(event) => match event {
                    RaftEvent::CheckElectionTimeOut(instant) => {
                        self.handle_election_timeout(instant);
                    }
                    RaftEvent::HeartBeatTimeOut() => {
                        self.handle_heartbeat_event();
                    }
                    RaftEvent::Vote(vote) => {
                        debug!("recv vote event");
                        let reply = self.handle_vote(&vote.request);
                        vote.reply.send(reply).expect("send vote reply error");
                    }
                    RaftEvent::VoteReply(reply_result) => {
                        self.handle_vote_reply(reply_result);
                    }
                    RaftEvent::Append(request, reply) => self.handle_append(request, reply),
                    RaftEvent::AppendReply { reply, peer_id } => {
                        self.handle_append_reply(reply, peer_id);
                    }
                    RaftEvent::ReadState(reply) => {
                        reply
                            .send(State {
                                p_state: self.get_persitent_state(),
                                v_state: self.get_volatitle_state(),
                            })
                            .expect("send read state reply failed");
                    }
                    RaftEvent::Stop => {}
                    RaftEvent::ClientCommand { data, reply } => self.handle_command(data, reply),
                },
            }
        }
    }
    // disable timer for test
    fn run_with_timer(mut self, enable_timer: bool) -> Sender<RaftEvent> {
        let res = self.tx.clone();
        self.update_election_check_time_out();
        if enable_timer {
            // set up timer
            self.set_up_election_timeout_checker();
            self.set_up_heartbeat_timer();
        }
        let _ = thread::spawn(move || self.main());
        res
    }

    fn run(self) -> Sender<RaftEvent> {
        self.run_with_timer(true)
    }

    fn becomes_candidate(&mut self) {
        self.persistent_state.term += 1;
        self.volatile_state.role = Role::CANDIDATOR;
        self.volatile_state.vote_record.clear();
        self.volatile_state.vote_record.insert(self.me as u64);

        self.update_election_check_time_out();
    }

    fn becomes_follower(&mut self, term: u64, vote_for: Option<u64>) {
        // change role to follower
        self.volatile_state.role = Role::FOLLOWER;
        // update term
        self.persistent_state.term = term;
        self.persistent_state.voted_for = vote_for;
        // update election timeout
        self.update_election_check_time_out();
    }

    fn handle_election_timeout(&mut self, instant: u64) {
        // self.set_up_election_timeout_checker();
        if self.volatile_state.time_to_check_election_time_out != instant {
            return;
        }
        if self.volatile_state.role == Role::LEADER {
            return;
        }
        info!("{} elclection timeout,start election", self.me);

        //  update state
        self.becomes_candidate();

        self.save_persist_state();
        self.send_vote(self.persistent_state.term);
    }
    fn handle_vote(&mut self, vote: &RequestVoteArgs) -> RequestVoteReply {
        let mut accept = false;
        // term is less than current term,refuse it
        let last_entry = self.persistent_state.logs.last().unwrap();
        let last_entry_term = last_entry.term;
        let last_entry_index = self.persistent_state.logs.len() - 1;
        if last_entry_term < vote.last_log_term
            || (last_entry_term == vote.last_log_term
                && last_entry_index as u64 <= vote.last_log_index)
        {
            // if term equal
            if vote.term == self.persistent_state.term {
                if self.volatile_state.role == Role::LEADER
                    || self.volatile_state.role == Role::FOLLOWER
                {
                    if self.persistent_state.voted_for.is_none() {
                        accept = true;
                        self.becomes_follower(vote.term, Some(vote.peer_id));
                        self.update_election_check_time_out();
                    } else if self.persistent_state.voted_for.unwrap() == vote.peer_id {
                        accept = true;
                        self.update_election_check_time_out();
                    }
                    // vote for not equal,refuse it
                }
            // if term bigger current
            } else if vote.term > self.persistent_state.term {
                self.becomes_follower(vote.term, Some(vote.peer_id));
                self.update_election_check_time_out();
                accept = true;
            };
        }
        return RequestVoteReply {
            peer_id: self.me as u64,
            term: self.persistent_state.term,
            vote_granted: accept,
        };
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term == self.persistent_state.term
            && self.volatile_state.role == Role::CANDIDATOR
            && reply.vote_granted
        {
            self.volatile_state.vote_record.insert(reply.peer_id);
            if self.volatile_state.vote_record.len() > self.peers.len() / 2 + 1 {
                self.become_leader();
                self.save_persist_state();
                self.send_append_to_all_client();
            }
        }
    }
    fn handle_append_reply(&mut self, reply: AppendReply, peer_id: u64) {
        // validate reply
        if self.volatile_state.role != Role::LEADER {
            info!("receive append reply but current role is not leader,ignore");
            return;
        }
        if reply.term < self.persistent_state.term {
            return;
        } else if reply.term == self.persistent_state.term {
            if reply.success {
                // update next_index
                let next_index = self.volatile_state.next_index.get_mut(&peer_id).unwrap();
                *next_index = reply.match_index + 1;
                let match_index = self.volatile_state.match_index.get_mut(&peer_id).unwrap();
                *match_index = reply.match_index;
                //  update commit index
                let mut match_indexs = vec![(self.persistent_state.logs.len() - 1) as u64];
                for (_, index) in &self.volatile_state.match_index {
                    match_indexs.push(*index);
                }
                match_indexs.sort();
                let n = match_indexs.get(match_indexs.len() / 2 + 1).unwrap();
                if *n > self.volatile_state.commit_index {
                    info!("update committed index to {}", *n);
                    self.volatile_state.commit_index = *n;
                }
            } else {
                // decrease next_index
                let i = self.volatile_state.next_index.get_mut(&peer_id).unwrap();
                *i = *i - 1;
                // resend append
                self.send_append_to(peer_id as usize, self.peers.get(peer_id as usize).unwrap());
            }
        } else {
            self.becomes_follower(reply.term, None);
        }
    }

    fn handle_append(&mut self, args: AppendArgs, reply_ch: oneshot::Sender<Result<AppendReply>>) {
        // validate term
        let reply = if args.term < self.persistent_state.term {
            AppendReply {
                term: self.persistent_state.term,
                success: false,
                match_index: 0,
            }
        } else {
            self.becomes_follower(args.term, None);
            let mut accept = false;

            // append/tructe log if match, or refuse log change
            let logs = &mut self.persistent_state.logs;
            if logs.len() < args.prev_log_index as usize {
                info!("prev entry not found, refuse append")
            } else if logs[args.prev_log_index as usize].term as u64 != args.prev_log_term {
                info!("prev entry not match,refuse append,index is {}, entry term is {}, args prev log term is {}",args.prev_log_index, logs[args.prev_log_index as usize].term ,args.term)
            } else {
                accept = true;
                while logs.len() - 1 > args.prev_log_index as usize {
                    logs.pop();
                }
                for data in args.entrys {
                    let entry = labcodec::decode::<Entry>(&data).expect("decode entry error");
                    logs.push(entry);
                }
            }
            self.save_persist_state();
            AppendReply {
                term: self.persistent_state.term,
                success: accept,
                match_index: (self.persistent_state.logs.len() - 1) as u64,
            }
        };

        reply_ch
            .send(Ok(reply))
            .expect("send append reply to chan failed");
    }
    fn handle_command(&mut self, data: Vec<u8>, reply: oneshot::Sender<ClientCommandReply>) {
        if self.volatile_state.role != Role::LEADER {
            reply
                .send(ClientCommandReply {
                    success: false,
                    index: 0,
                    term: 0,
                })
                .expect("send command reply error");
        } else {
            let entry = Entry {
                data,
                term: self.persistent_state.term,
            };
            self.persistent_state.logs.push(entry);
            self.send_append_to_all_client();
            self.volatile_state.last_send_append_time = system_time_now_epoch();

            reply
                .send(ClientCommandReply {
                    success: true,
                    index: (self.persistent_state.logs.len() - 1) as u64,
                    term: self.persistent_state.term,
                })
                .expect("send command reply error");
        }
    }

    fn handle_heartbeat_event(&mut self) {
        if self.volatile_state.role != Role::LEADER {
            return;
        }
        // check last append send
        let now = system_time_now_epoch();
        if now - self.volatile_state.last_send_append_time >= HEARTBEAT_CHECK_INTERVAL {
            self.send_append_to_all_client();
            self.volatile_state.last_send_append_time = now;
        }
    }

    fn become_leader(&mut self) {
        info!("get votes from majority ,become leader");
        self.volatile_state.role = Role::LEADER;
        self.volatile_state.next_index.clear();
        self.volatile_state.match_index.clear();
        for (id, _) in self.peers.iter().enumerate() {
            self.volatile_state
                .next_index
                .insert(id as u64, (self.last_log_index() + 1) as u64);
            self.volatile_state.match_index.insert(id as u64, 0);
        }
        self.volatile_state.last_send_append_time = system_time_now_epoch();
    }
    fn set_up_heartbeat_timer(&mut self) {
        // set up a timer
        // send a heartbeat check event
        // check if need to send a heartbeat in constant time interval
        let tx = self.tx.clone();
        spawn(move || loop {
            thread::sleep(Duration::from_millis(HEARTBEAT_CHECK_INTERVAL));
            tx.send(RaftEvent::HeartBeatTimeOut())
                .expect("send heartbeat check event error");
        });
    }

    fn set_up_election_timeout_checker(&mut self) {
        let check_time = self.volatile_state.time_to_check_election_time_out;
        let now = system_time_now_epoch();
        let duration = if check_time > now {
            check_time - now
        } else {
            0
        };

        debug!("set next time out check after {} ms", duration);
        let tx = self.tx.clone();
        let _join = spawn(move || {
            thread::sleep(Duration::from_millis(duration));
            tx.send(RaftEvent::CheckElectionTimeOut(check_time))
                .expect("send time out check error");
            debug!("send time out check event");
        });
    }

    fn prev_log_index(&self, id: usize) -> u64 {
        let res = self
            .volatile_state
            .next_index
            .get(&(id as u64))
            .expect("should inited");
        *res - 1
    }
    fn prev_log_term(&self, id: usize) -> u64 {
        let index = self.prev_log_index(id);
        if index == 0 {
            return 0;
        }
        let entry = &self.persistent_state.logs[index as usize];
        return entry.term;
    }

    fn send_append_to_all_client(&mut self) {
        for (peer_id, c) in self.peers.iter().enumerate() {
            if peer_id == self.me {
                continue;
            }
            self.send_append_to(peer_id, c);
        }
    }

    fn send_append_to(&self, peer_id: usize, c: &RaftClient) {
        let match_index = self
            .volatile_state
            .match_index
            .get(&(peer_id as u64))
            .expect("get match index fail");

        assert!(*match_index < self.persistent_state.logs.len() as u64);

        let mut entrys = vec![];
        for i in (*match_index as usize + 1)..self.persistent_state.logs.len() {
            let mut data = vec![];
            let entry = self.persistent_state.logs.get(i).unwrap();
            labcodec::encode(entry, &mut data).expect("encode entry fail");
            entrys.push(data);
        }

        let append_request = AppendArgs {
            leader_id: self.me as u64,
            term: self.persistent_state.term,
            prev_log_index: self.prev_log_index(peer_id),
            prev_log_term: self.prev_log_term(peer_id),
            leader_commit: self.volatile_state.commit_index,
            entrys,
        };
        debug!("send append request to peer {}", peer_id);
        let c_clone = c.clone();
        let tx = self.tx.clone();
        c.spawn(async move {
            let res = c_clone.append(&append_request).await;
            match res {
                Err(e) => {
                    warn!("send heartbeat append request failed {:?} ", e);
                    return;
                }
                Ok(reply) => {
                    let res = tx.send(RaftEvent::AppendReply {
                        reply,
                        peer_id: peer_id as u64,
                    });
                    if res.is_err() {
                        warn!("send heartbeat append request failed {:?}", res);
                    }
                }
            }
        });
    }

    // compare last heart to current, start vote if time out pass
    fn send_vote(&mut self, term: u64) {
        // send vote request
        let request = RequestVoteArgs {
            peer_id: self.me as u64,
            term: term,
            last_log_index: self.last_log_index() as u64,
            last_log_term: self.last_log_term() as u64,
        };
        for (peer_id, _) in self.peers.iter().enumerate() {
            if peer_id == self.me {
                continue;
            }
            debug!("send vote request to peer {}", peer_id);
            self.send_request_vote(peer_id, request.clone(), self.tx.clone());
        }
    }
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            info!("raft persist is empty");
            return;
        }
        let res = labcodec::decode::<PersistentState>(data);
        match res {
            Ok(o) => self.persistent_state = o,
            Err(e) => {
                panic!("failed to decode raft pesiste data: {:?}", e)
            }
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
        result_tx: Sender<RaftEvent>,
    ) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            match res {
                Err(e) => {
                    warn!("send vote error {:?}", e);
                    return;
                }
                Ok(reply) => {
                    let res = result_tx.send(RaftEvent::VoteReply(reply));
                    if res.is_err() {
                        warn!("send resp error {:?}", res);
                        return;
                    }
                }
            }
        });
        // rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }

    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        // let a = channel();
        // let _ = self.send_request_vote(0, Default::default(), a.0);
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }

    pub fn get_persitent_state(&self) -> PersistentState {
        self.persistent_state.clone()
    }
    pub fn get_volatitle_state(&self) -> VolatileState {
        self.volatile_state.clone()
    }

    // index begin from 0 on start
    pub fn last_log_index(&self) -> usize {
        self.persistent_state.logs.len() - 1
    }
    // term begin from 0 on start
    pub fn last_log_term(&self) -> u64 {
        let e = self.persistent_state.logs.last();
        match e {
            None => 0,
            Some(a) => a.term,
        }
    }
    pub fn is_election_time_out(&self, time: u64) -> bool {
        self.volatile_state
            .time_to_check_election_time_out
            .eq(&time)
    }

    pub fn save_persist_state(&mut self) {
        let mut data = Vec::new();
        self.persistent_state
            .encode(&mut data)
            .expect("encode failed");
        self.persister.save_raft_state(data);
    }
}

#[derive(Debug, Clone)]
enum RaftReply {
    VoteReply(RequestVoteReply),
}

#[derive(Debug)]
struct VoteRequestEvent {
    request: RequestVoteArgs,
    reply: oneshot::Sender<RequestVoteReply>,
}
enum RequestEvent {
    Vote(VoteRequestEvent),
}
#[derive(Clone, Debug)]
struct ClientCommandReply {
    success: bool,
    index: u64,
    term: u64,
}

// |election 超时|heartbeat timeout|append|append apply|vote|vote apply|
#[derive(Debug)]
enum RaftEvent {
    CheckElectionTimeOut(u64),
    HeartBeatTimeOut(),
    Append(AppendArgs, oneshot::Sender<Result<AppendReply>>),
    AppendReply {
        reply: AppendReply,
        peer_id: u64,
    },
    VoteReply(RequestVoteReply),
    Vote(VoteRequestEvent),

    Stop,
    ReadState(Sender<State>),

    ClientCommand {
        data: Vec<u8>,
        reply: oneshot::Sender<ClientCommandReply>,
    },
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    tx: Arc<async_lock::Mutex<Sender<RaftEvent>>>, // Your code here.
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // let handler = RaftHandler::new(raft);
        let tx = raft.run();
        Node {
            tx: Arc::new(async_lock::Mutex::new(tx)),
        }
    }

    // for test
    fn new_disable_timer(raft: Raft) -> Node {
        // Your code here.
        // let handler = RaftHandler::new(raft);
        let tx = raft.run_with_timer(false);
        Node {
            tx: Arc::new(async_lock::Mutex::new(tx)),
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)

        let mut data = vec![];
        command.encode(&mut data);
        let (tx, rx) = oneshot::channel();

        block_on(async {
            let g = self.tx.lock().await;
            let res = g.send_timeout(
                RaftEvent::ClientCommand {
                    data: data,
                    reply: tx,
                },
                Duration::from_millis(10),
            );
            res.expect("send client timeout ");
        });

        let res = block_on(async {
            let receive_res = rx.await;
            let res = receive_res.expect("receive client command error");
            res
        });
        if res.success {
            debug!("log is {:?}", res.index);
            Ok((res.index, res.term))
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().p_state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().v_state.role == Role::LEADER
    }

    // for test
    fn send_event(&self, event: RaftEvent) {
        let t = block_on(self.tx.lock());
        t.send(event).expect("send read state request error");
    }
    fn get_state(&self) -> State {
        let t = block_on(self.tx.lock());
        let (tx, rx) = crossbeam_channel::unbounded();
        t.send(RaftEvent::ReadState(tx))
            .expect("send read state request error");
        let res = rx.recv().expect("read state error");
        res
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args)
        info!("receive vote from peer {},term {}", args.peer_id, args.term);
        let (tx, rx) = oneshot::channel();
        let vote_event = VoteRequestEvent {
            request: args,
            reply: tx,
        };
        let guard = self.tx.lock().await;
        guard
            .send(RaftEvent::Vote(vote_event))
            .expect("send vote request failed");

        let t = rx.await;
        match t {
            Ok(res) => labrpc::Result::Ok(res),
            Err(e) => labrpc::Result::Err(labrpc::Error::Other(e.to_string())),
        }
    }
    async fn append(&self, args: AppendArgs) -> labrpc::Result<AppendReply> {
        {
            // Your code here (2A, 2B).
            // crate::your_code_here(args)
            info!("receive append from peer {}", args.leader_id);
            let (tx, rx) = oneshot::channel();
            let append_event = RaftEvent::Append(args, tx);
            let guard = self.tx.lock().await;
            guard
                .send(append_event)
                .expect("send append request failed");

            let t = rx.await;
            match t {
                Ok(res) => match res {
                    Ok(r) => labrpc::Result::Ok(r),
                    Err(e) => labrpc::Result::Err(labrpc::Error::Other(e.to_string())),
                },
                Err(e) => labrpc::Result::Err(labrpc::Error::Other(e.to_string())),
            }
        }
    }
}

fn system_time_now_epoch() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis() as u64
}

#[cfg(test)]
pub mod my_tests {
    use std::{
        assert_eq,
        convert::TryInto,
        panic, println, process,
        sync::{Arc, Mutex, Once},
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        todo, vec,
    };

    use futures::{
        channel::{
            mpsc::{channel, unbounded, UnboundedReceiver},
            oneshot,
        },
        executor::{block_on, ThreadPool},
        FutureExt, SinkExt, StreamExt, TryFutureExt,
    };
    use labcodec::Message;
    use labrpc::{Client, Rpc};

    use super::{
        persister::SimplePersister, system_time_now_epoch, Entry, Node, PersistentState, Raft,
    };
    use crate::{proto::raftpb::*, raft::HEARTBEAT_CHECK_INTERVAL};
    use crate::{
        proto::{kvraftpb::GetReply, raftpb::RequestVoteReply},
        raft::{
            RaftEvent, Role, State, VolatileState, ELECTION_TIMEOUT_MIN_TIME,
            ELECTION_TIMEOUT_RANGE,
        },
    };

    fn init_logger() {
        static LOGGER_INIT: Once = Once::new();
        LOGGER_INIT.call_once(env_logger::init);
    }

    fn crash_process_when_any_thread_panic() {
        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));
    }
    fn create_raft_client(name: &str) -> (RaftClient, UnboundedReceiver<Rpc>) {
        let (tx, rx) = unbounded();
        let c = Client {
            name: String::from(name),
            sender: tx,
            hooks: Arc::new(Mutex::new(None)),
            worker: ThreadPool::new().unwrap(),
        };
        let raft_client = RaftClient::new(c);
        (raft_client, rx)
    }

    fn create_raft_with_client(clients: Vec<RaftClient>, me: usize) -> Raft {
        init_logger();
        crash_process_when_any_thread_panic();
        let p = SimplePersister::new();
        let (tx, _) = unbounded();
        Raft::new(clients, me, Box::new(p), tx)
    }
    fn create_raft() -> Raft {
        create_raft_with_client(vec![], 0)
    }

    fn create_node() -> Node {
        Node::new(create_raft())
    }
    #[test]
    fn test_update_check_time_out() {
        let mut s = create_raft();
        let now = system_time_now_epoch();
        s.update_election_check_time_out();
        let t = s.volatile_state.time_to_check_election_time_out;
        assert!(t - now >= 150);
        assert!(t - now <= 300);
    }

    #[test]
    fn test_rand_time_out() {
        for _ in 0..10 {
            let res = Raft::random_timeout();
            assert!(res.as_millis() >= 150);
            assert!(res.as_millis() <= 300);
        }
    }
    #[test]
    fn test_init_state() {
        let n = create_node();
        let state = n.get_state();
        let now = system_time_now_epoch();
        assert!(state.v_state.role == Role::FOLLOWER);
        assert!(state.p_state.term == 0);
        assert!(state.v_state.time_to_check_election_time_out - now > 140);
        assert!(state.v_state.time_to_check_election_time_out - now <= 300);
    }
    // []超时开始选举
    #[test]
    fn test_election_timeout() {
        let (c, rx) = create_raft_client("test");
        let me = 1;
        let new_term = 1;

        let r = create_raft_with_client(vec![c], me);
        let n = Node::new_disable_timer(r);

        let state = n.get_state();
        assert_eq!(state.v_state.role, Role::FOLLOWER);
        assert_eq!(state.p_state.term, 0);
        let time = state.v_state.time_to_check_election_time_out;

        // 超时
        n.send_event(RaftEvent::CheckElectionTimeOut(time));
        // assert vote
        let (reply, rx) = get_send_rpc::<RequestVoteArgs>(rx);
        assert_eq!(reply.peer_id, me as u64);
        assert_eq!(reply.term, new_term);
        assert_eq!(reply.last_log_index, 0);
        assert_eq!(reply.last_log_term, 0);

        // assert state
        let state = n.get_state();

        assert_eq!(state.v_state.role, Role::CANDIDATOR);
        assert_eq!(state.p_state.term, new_term);
        assert_eq!(state.v_state.vote_record.len(), 1);
        assert!(state.v_state.time_to_check_election_time_out != time);
        assert!(state.v_state.vote_record.get(&(me as u64)).is_some());
    }
    #[test]

    // 选举成功，发送心跳
    fn test_candidate_election_success() {
        let (c, rx) = create_raft_client("test");
        let me = 1;
        let new_term = 1;

        let mut r = create_raft_with_client(vec![c], me);
        r.becomes_candidate();
        let n = Node::new_disable_timer(r);

        n.send_event(RaftEvent::VoteReply(RequestVoteReply {
            term: new_term,
            vote_granted: true,
            peer_id: 0,
        }));
        let (reply, rx) = get_send_rpc::<AppendArgs>(rx);
        assert_eq!(reply.term, new_term);
        assert_eq!(reply.leader_id, me as u64);
        assert_eq!(reply.leader_id, me as u64);
        assert_eq!(reply.prev_log_index, 0);
        assert_eq!(reply.prev_log_term, 0);
        assert_eq!(reply.leader_commit, 0);
        let state = n.get_state();
        assert_eq!(state.v_state.role, Role::LEADER);
    }
    // election vote timeout, no node elect successfull
    // 选举失败，没有达到半数，继续下一轮
    #[test]
    fn test_election_no_winnner() {
        let (c, rx) = create_raft_client("test");
        let me = 1;

        let mut r = create_raft_with_client(vec![c], me);
        r.becomes_candidate();
        let n = Node::new(r);
        let state = n.get_state();
        assert_eq!(state.v_state.role, Role::CANDIDATOR);
        let old_term = state.p_state.term;

        // sleep after  election timeout
        thread::sleep(Duration::from_millis(320));
        let state = n.get_state();
        let role = state.v_state.role;
        assert_eq!(role, Role::CANDIDATOR);
        assert_eq!(state.p_state.term, old_term + 1);
        let (_reply) = get_send_rpc::<RequestVoteArgs>(rx);
    }
    //  followe/leader 接受选举
    #[test]
    fn test_follower_accept_election() {
        let (c, rx) = create_raft_client("test");
        let me = 1;
        let mut r = create_raft_with_client(vec![c], me);
        r.becomes_follower(0, None);
        let n = Node::new(r);
        let new_term = 2;
        let leader_id = 0;
        let reply = block_on(async {
            n.request_vote(RequestVoteArgs {
                peer_id: leader_id,
                term: new_term,
                last_log_index: 0,
                last_log_term: 0,
            })
            .await
        })
        .unwrap();

        let state = n.get_state();
        assert_eq!(state.p_state.term, new_term);
        assert_eq!(state.p_state.voted_for.unwrap(), leader_id);

        assert_eq!(reply.term, new_term);
        assert_eq!(reply.vote_granted, true);
        assert_eq!(reply.peer_id as usize, me);
    }
    // [] followe/leader 拒绝选举 已经投票/term
    #[test]
    fn test_election_refuse_already_vote() {
        let (c, rx) = create_raft_client("test");
        let me = 3;
        let mut r = create_raft_with_client(vec![c], me);
        // vote for 5
        let _ = r.persistent_state.voted_for.insert(5);
        r.becomes_candidate();
        let n = Node::new_disable_timer(r);

        let (tx, rx) = oneshot::channel::<RequestVoteReply>();
        n.send_event(RaftEvent::Vote(super::VoteRequestEvent {
            request: RequestVoteArgs {
                peer_id: 4,
                term: 1,
                last_log_index: 0,
                last_log_term: 0,
            },
            reply: tx,
        }));

        let res = block_on(async { rx.await }).unwrap();
        assert_eq!(res.vote_granted, false)
    }

    //  followe/leader 拒绝选举 logs不够新
    #[test]
    pub fn test_election_follower_refuse_log_is_old() {
        let (c, rx) = create_raft_client("test");
        let me = 3;
        let mut r = create_raft_with_client(vec![c], me);

        let logs = &mut r.persistent_state.logs;
        logs.push(Entry {
            data: vec![1],
            term: 1,
        });
        logs.push(Entry {
            data: vec![1],
            term: 1,
        });
        logs.push(Entry {
            data: vec![1],
            term: 2,
        });

        let n = Node::new_disable_timer(r);

        let (tx, rx) = oneshot::channel::<RequestVoteReply>();
        n.send_event(RaftEvent::Vote(super::VoteRequestEvent {
            request: RequestVoteArgs {
                peer_id: 4,
                term: 3,
                last_log_index: 2,
                last_log_term: 2,
            },
            reply: tx,
        }));

        let res = block_on(async { rx.await }).unwrap();
        assert_eq!(res.vote_granted, false)
    }

    // 选举失败，其他节点成功
    #[test]
    fn test_election_but_fail() {
        let (c, rx) = create_raft_client("test");
        let me = 3;

        let mut r = create_raft_with_client(vec![c], me);
        r.becomes_candidate();
        let n = Node::new(r);
        let state = n.get_state();
        let term = state.p_state.term;
        let old_check_time = state.v_state.time_to_check_election_time_out;

        let (tx, rx) = oneshot::channel::<super::Result<AppendReply>>();

        n.send_event(RaftEvent::Append(
            AppendArgs {
                leader_id: 0,
                term: term,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                entrys: vec![],
            },
            tx,
        ));

        let state = n.get_state();
        assert_eq!(state.p_state.term, term);
        assert_eq!(state.v_state.role, Role::FOLLOWER);
        assert!(state.v_state.time_to_check_election_time_out != old_check_time);
    }

    #[test]
    fn test_receive_append() {
        let (c, rx) = create_raft_client("test");
        let me = 3;

        let mut r = create_raft_with_client(vec![c], me);
        let term = 1;
        let leader_id = 1;
        r.becomes_follower(term, None);
        let n = Node::new_disable_timer(r);
        let store_data = vec![1, 2, 3];

        // [x] append 接受term
        let e = Entry {
            data: store_data.clone(),
            term: term,
        };
        let mut data = Vec::new();
        labcodec::encode(&e, &mut data).unwrap();

        // append two entries in two append requeset
        let append_request = AppendArgs {
            leader_id,
            term,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: leader_id,
            entrys: vec![data],
        };
        let (tx, rx) = oneshot::channel::<super::Result<AppendReply>>();
        n.send_event(RaftEvent::Append(append_request, tx));
        // let reply = block_on(async { rx.into_future().await }).unwrap();

        let state = n.get_state();
        let log = &state.p_state.logs;

        assert_eq!(log.len(), 2);

        let append_e = log.get(1).unwrap();
        assert_eq!(&e.term, &term);
        assert_eq!(&e.data, &store_data);

        // [x]失败，term不符合当前
        let append_request = AppendArgs {
            leader_id,
            term: term - 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: leader_id,
            entrys: vec![],
        };

        let (tx, mut rx) = oneshot::channel::<super::Result<AppendReply>>();
        n.send_event(RaftEvent::Append(append_request, tx));

        let res = block_on(async { rx.await }).unwrap().unwrap();
        assert_eq!(res.success, false);
        assert_eq!(res.term, term);

        // 拒绝，prex不符合
        let append_request = AppendArgs {
            leader_id,
            term: term,
            prev_log_index: 0,
            prev_log_term: 1,
            leader_commit: leader_id,
            entrys: vec![],
        };
        let (tx, mut rx) = oneshot::channel::<super::Result<AppendReply>>();
        n.send_event(RaftEvent::Append(append_request, tx));

        let res = block_on(async { rx.await }).unwrap().unwrap();
        assert_eq!(res.success, false);
        assert_eq!(res.term, term);
        // append 截断 follower
        let e = Entry {
            data: store_data.clone(),
            term: term,
        };
        let mut data = Vec::new();
        labcodec::encode(&e, &mut data).unwrap();
        let entrys = vec![data.clone(), data];

        let append_request = AppendArgs {
            leader_id,
            term: term,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: leader_id,
            entrys,
        };
        let (tx, mut rx) = oneshot::channel::<super::Result<AppendReply>>();
        n.send_event(RaftEvent::Append(append_request, tx));
        let res = block_on(async { rx.await }).unwrap().unwrap();
        assert_eq!(res.success, true);
        assert_eq!(res.term, term);

        let state = n.get_state();
        let logs = &state.p_state.logs;
        assert_eq!(logs.len(), 3);
        let entry = logs.get(2).unwrap();
        assert_eq!(entry.data, store_data);
    }
    // leader 超时心跳发送
    #[test]
    fn test_send_heartbeat() {
        let (c, rx) = create_raft_client("test");
        let me = 3;

        let mut r = create_raft_with_client(vec![c], me);
        let term = 1;
        let state = &mut r.persistent_state;
        state.term = term;
        r.become_leader();
        let n = Node::new(r);
        // sleep and check heartbeat

        thread::sleep(Duration::from_millis(HEARTBEAT_CHECK_INTERVAL + 10));
        let (append, rx) = get_send_rpc::<AppendArgs>(rx);

        assert_eq!(append.entrys.len(), 0);
        assert_eq!(append.leader_id, 3);
        assert_eq!(append.term, term);
    }
    // append client 请求成功
    #[test]
    fn test_client_command_success() {
        let (c, rx) = create_raft_client("test");
        let me = 1;
        let mut r = create_raft_with_client(vec![c], me);
        // leader start in 1
        r.persistent_state.term = 1;
        r.become_leader();
        let node = Node::new_disable_timer(r);

        let command = MessageForTest { data: 1 };

        let res = node.start(&command);
        assert!(res.is_ok());
        let r = res.unwrap();
        assert_eq!(r.0, 1);
        assert_eq!(r.1, 1);
    }

    #[test]
    fn test_client_command_to_non_leader() {
        let (c, rx) = create_raft_client("test");
        let me = 1;
        let mut r = create_raft_with_client(vec![c], me);
        r.becomes_candidate();
        let node = Node::new_disable_timer(r);

        let command = MessageForTest { data: 1 };

        let res = node.start(&command);
        assert!(res.is_err());
    }
    // [x]append reply 成功leader 更新commit,match_index,next_index
    #[test]
    fn test_commited_index_update() {
        let (c1, rx_1) = create_raft_client("test");
        let (c2, rx_2) = create_raft_client("test");
        let me = 3;

        let mut r = create_raft_with_client(vec![c1, c2], me);
        let term = 1;
        r.persistent_state.term = term;
        r.become_leader();
        let n = Node::new_disable_timer(r);
        let command = MessageForTest { data: 1 };

        let _ = n.start(&command);
        let state = n.get_state();
        let (append, rx) = get_send_rpc::<AppendArgs>(rx_1);
        assert_eq!(append.prev_log_index, 0);
        assert_eq!(append.entrys.len(), 1);
        assert_eq!(state.v_state.commit_index, 0);
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term: term,
                success: true,
                match_index: 1,
            },
            peer_id: 0,
        });
        let state = n.get_state();
        assert_eq!(state.v_state.commit_index, 1);
        assert_eq!(*state.v_state.next_index.get(&0).unwrap(), 2);
        assert_eq!(*state.v_state.match_index.get(&0).unwrap(), 1);
    }
    #[test]
    fn debug() {
        let (c, rx) = create_raft_client("test");
        let args = RequestVoteArgs {
            peer_id: 1,
            term: 3,
            last_log_index: 2,
            last_log_term: 2,
        };
        c.request_vote(&args);
        let res = get_send_rpc::<RequestVoteReply>(rx);

        println!("res is {:?}", res);

        let p = PersistentState::default();
        println!("p is {:?}", p);
        let v = VolatileState::default();
        println!("v is {:?}", v);
    }

    fn get_send_rpc<T: Message>(mut rx: UnboundedReceiver<Rpc>) -> (T, UnboundedReceiver<Rpc>) {
        let (res_tx, res_rx) = crossbeam_channel::unbounded();
        let t = thread::spawn(move || {
            block_on(async {
                let res = rx.next().await.unwrap();
                res_tx.send(res).unwrap();
                rx
            })
        });
        thread::sleep(Duration::from_millis(20));

        let date = res_rx.try_recv().expect("not receive rpc").req.unwrap();
        let res = labcodec::decode::<T>(&date).unwrap();
        let rx = t.join().unwrap();
        debug!("receive rpc {:?}", res);
        (res, rx)
    }
}
