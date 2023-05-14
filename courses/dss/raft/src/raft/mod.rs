use core::panic;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::channel;
use futures::channel::mpsc::unbounded;
use futures::channel::oneshot;
use futures::channel::oneshot::channel;
use futures::executor::block_on;
use futures::inner_macro::select;
use log::debug;
use log::error;
use log::info;
use prost::Message;
use std::ops::Add;
use std::sync::Arc;
use std::thread;
use std::thread::spawn;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::todo;

use futures::channel::mpsc::UnboundedSender;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

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
    CANDIDATOR = 1,
    FOLLOWER = 2,
}

#[derive(Clone, Message)]
pub struct Entry {
    #[prost(bytes, tag = "1")]
    data: Vec<u8>,
    #[prost(uint64, tag = "2")]
    term: u64,
}

/// State of a raft peer.
#[derive(Clone, prost::Message)]
pub struct State {
    // persistent state
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub voted_for: u64,
    #[prost(message, repeated, tag = "3")]
    pub logs: Vec<Entry>,
    // volatile state
    #[prost(enumeration = "Role", tag = "4")]
    pub role: i32,
    #[prost(uint64, tag = "5")]
    pub commit_index: u64,
    #[prost(uint64, tag = "6")]
    pub last_applied: u64,
    // update to current_time+random_time[150,300)
    // when to update:
    // init
    // receive any messages from valid leader/candidate
    #[prost(uint64, tag = "7")]
    pub time_to_check_election_time_out: u64,
}

// todo
// impl Default for State {
//     fn default() -> State {
//         let time_out_check_time = Instant::now().add(Self::random_timeout());
//         State {
//             commit_index: 0,
//             last_applied: 0,
//             voted_for: 0,
//             role: Role::FOLLOWER,
//             term: 0,
//             logs: Vec::new(),
//             time_to_check_election_time_out: time_out_check_time,
//         }
//     }
// }

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_role() == Role::LEADER
    }
    fn random_timeout() -> Duration {
        Duration::from_millis(150 + rand::random::<u64>() % 150)
    }

    // set check_election_time to now+random_timeout
    pub fn update_check_time_out(&mut self) {
        let start = SystemTime::now();
        start.add(Self::random_timeout());
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let t = since_the_epoch.as_millis() as u64;
        self.time_to_check_election_time_out = t;
    }

    pub fn get_role(&self) -> Role {
        match self.role {
            0 => Role::LEADER,
            2 => Role::FOLLOWER,
            1 => Role::CANDIDATOR,
            _ => {
                panic!("get role is not supported")
            }
        }
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
    state: State,
    // other state

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

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // todo
        // crate::your_code_here((rf, apply_ch))
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
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
        let id = self.me;
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if res.is_err() {
                error!("send vote error {:?}", res);
            }
            let res = result_tx.send(RaftEvent::VoteReply(res));
            if res.is_err() {
                error!("send resp error {:?}", res);
                return;
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
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }

    pub fn get_state(&self) -> State {
        self.state.clone()
    }
    // pub fn set_role(&mut self, role: Role) {
    // self.role = role;
    // }

    // index begin from 0 on start
    pub fn last_log_index(&self) -> usize {
        self.state.logs.len()
    }
    // term begin from 0 on start
    pub fn last_log_term(&self) -> u64 {
        let e = self.state.logs.last();
        match e {
            None => 0,
            Some(a) => a.term,
        }
    }
    pub fn is_time_out(&self, time: u64) -> bool {
        self.state.time_to_check_election_time_out.eq(&time)
    }

    pub fn update_state_and_persist(&mut self, state: State) {
        self.state = state;
        let mut data = Vec::new();
        self.state.encode(&mut data).expect("encode failed");
        self.persister.save_raft_state(data);
    }
}

#[derive(Debug, Clone)]
enum RaftReply {
    VoteReply(Result<RequestVoteReply>),
}

#[derive(Debug)]
struct VoteRequestEvent {
    request: RequestVoteArgs,
    reply: oneshot::Sender<Result<RequestVoteReply>>,
}
enum RequestEvent {
    Vote(VoteRequestEvent),
}
#[derive(Debug)]
enum RaftEvent {
    LeaderTimeOutCheck(u64),
    ReadState(Sender<State>),
    VoteReply(Result<RequestVoteReply>),
    Vote(VoteRequestEvent),
    Stop,
}

// handle raft event ,start_read/time out/vote request
struct RaftHandler {
    rx: Receiver<RaftEvent>,
    tx: Sender<RaftEvent>,
    raft: Box<Raft>,
}

impl RaftHandler {
    pub fn new(raft: Raft) -> Self {
        use crossbeam_channel::unbounded;
        let (tx, rx) = unbounded::<RaftEvent>();

        let res = RaftHandler {
            rx,
            tx,
            raft: Box::new(raft),
        };
        res
    }

    pub fn start(mut self) -> Sender<RaftEvent> {
        let tx = self.tx.clone();
        let tx_res = tx.clone();

        let _ = thread::spawn(move || self.main());
        tx_res
    }

    fn main(mut self) {
        self.raft.state.update_check_time_out();
        self.set_up_timeout_check_timer();

        loop {
            //
            let res = self.rx.recv();
            info!("receive raft event {:?}", res);
            match res {
                Err(RecvError) => {
                    info!("raft handler chan receive error {},stop handle", RecvError);
                    return;
                }
                Ok(event) => {
                    let state = self.raft.state.clone();
                    match event {
                        RaftEvent::LeaderTimeOutCheck(instant) => {
                            info!(
                                "recv leader timeout event,id is {},role is {:?}",
                                self.raft.me, state.role
                            );
                            self.handle_leader_time_out(instant, state);
                        }
                        RaftEvent::Vote(vote) => {
                            debug!("recv vote event");
                            let reply = self.handle_vote(vote.request);
                            vote.reply.send(Ok(reply)).expect("send vote reply failed");
                        }
                        RaftEvent::VoteReply(reply_result) => match reply_result {
                            Ok(reply) => {
                                self.handle_vote_reply(reply);
                            }
                            Err(err) => {
                                error!("recieved vote reply error {:?}", err);
                            }
                        },
                        _ => {
                            info!("main recieved others event,just ignore");
                        }
                    }
                }
            }
        }
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply) {
        todo!()
    }
    fn handle_vote(&mut self, request: RequestVoteArgs) -> RequestVoteReply {
        todo!()
    }

    fn handle_leader_time_out(&mut self, instant: u64, state: State) {
        if state.get_role() == Role::LEADER {
            info!(
                "{} recv leader time out,but role is leader,just ignore",
                self.raft.me
            );
            return;
        }
        let is_time_out = self.raft.is_time_out(instant);
        if !is_time_out {
            return;
        }
        info!("time out ,start vote ");
        // update state
        let mut new_state = state.clone();
        new_state.update_check_time_out();
        new_state.set_role(Role::CANDIDATOR);
        self.raft.update_state_and_persist(new_state);
        self.set_up_timeout_check_timer();
        // send vote
        self.send_vote(state.term);
    }

    fn set_up_timeout_check_timer(&mut self) {
        let check_time = self.raft.state.time_to_check_election_time_out;
        let now = system_time_now_epoch();
        let duration = if check_time > now {
            check_time - now
        } else {
            0
        };

        // let time = check_time.clone();
        debug!("set next time out check after {} ms", duration);
        let tx = self.tx.clone();
        let _join = spawn(move || {
            thread::sleep(Duration::from_millis(duration));
            tx.send(RaftEvent::LeaderTimeOutCheck(check_time))
                .expect("send time out check error");
            info!("send time out check");
        });
    }

    // compare last heart to current, start vote if time out pass
    fn send_vote(&mut self, term: u64) {
        // send vote request
        let request = RequestVoteArgs {
            peer_id: self.raft.me as u64,
            term: term,
            last_log_index: self.raft.last_log_index() as u64,
            last_log_term: self.raft.last_log_term() as u64,
        };
        for (peer_id, _) in self.raft.peers.iter().enumerate() {
            if peer_id == self.raft.me {
                continue;
            }
            debug!("send vote request to peer {}", peer_id);
            self.raft
                .send_request_vote(peer_id, request.clone(), self.tx.clone());
        }
    }
    fn check_time_out(last_heart_beat_time: SystemTime, time_out_duration: Duration) -> bool {
        let now = SystemTime::now();
        let is_time_out = now
            .duration_since(last_heart_beat_time)
            .expect("get duration time error")
            .cmp(&time_out_duration)
            .is_lt();
        is_time_out
    }
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
        let handler = RaftHandler::new(raft);
        let tx = handler.start();
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
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().get_role() == Role::LEADER
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
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
        drop(guard);

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

fn system_time_now_epoch() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let t = since_the_epoch.as_millis() as u64;
    t
}

#[cfg(test)]
pub mod my_tests {
    use std::{
        panic, println, process,
        sync::Once,
        thread,
        time::{self, Duration, Instant, SystemTime, UNIX_EPOCH},
        vec,
    };

    use futures::channel::mpsc::{unbounded, UnboundedReceiver};

    use crate::{proto::raftpb::RequestVoteReply, raft::State};

    use super::{persister::SimplePersister, ApplyMsg, Node, Raft, RaftHandler};

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

    fn create_raft_hander() -> RaftHandler {
        init_logger();
        crash_process_when_any_thread_panic();
        let p = SimplePersister::new();
        let (tx, _) = unbounded();
        let raft = Raft::new(vec![], 0, Box::new(p), tx);
        let raft_handle = RaftHandler::new(raft);
        raft_handle
    }

    #[test]
    fn test_start() {
        init_logger();
        let p = SimplePersister::new();
        let (tx, _) = unbounded();
        let raft = Raft::new(vec![], 0, Box::new(p), tx);
        let raft_handle = RaftHandler::new(raft);
        raft_handle.start();
        thread::sleep(Duration::from_millis(2000));
    }
    #[test]
    fn test_rand_time_out() {
        for _ in 0..10 {
            let res = State::random_timeout();
            assert!(res.as_millis() >= 150);
            assert!(res.as_millis() <= 300);
        }
    }
    #[test]
    fn debug_initial_election_2a() {
        crash_process_when_any_thread_panic();
        let servers = 3;

        use crate::raft::config::Config;
        let mut cfg = Config::new(servers);

        cfg.begin("Test (2A): initial election");

        loop {}
    }
    #[test]
    fn test_follow_election_timeout() {}
    #[test]
    fn test_follow_receive_vote() {}
    #[test]
    fn test_follow_receive_append() {}
    #[test]
    fn test_candidate_timeout() {}

    // #[test]
    // fn debug() {
    //     let t = SystemTime::now();
    //     thread::sleep(Duration::from_millis(20));
    //     let d: SystemTime = SystemTime::now();
    //     let d = d.duration_since(t);
    //     println!("time is {:?} ms", d);

    //     let s = State::default();

    //     let start = SystemTime::now();
    //     let since_the_epoch = start
    //         .duration_since(UNIX_EPOCH)
    //         .expect("Time went backwards");
    //     let in_ms = since_the_epoch.as_millis() as u64;
    //     println!("in_ms is {:?} ms", in_ms);
    // }

    // is a leader elected?
    // cfg.check_one_leader();
}
