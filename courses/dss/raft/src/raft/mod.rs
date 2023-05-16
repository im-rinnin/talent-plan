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
use std::collections::HashSet;
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

/// State of a raft peer.
#[derive(Clone, prost::Message)]
pub struct State {
    // persistent state
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(int64, tag = "2")]
    // -1 not vote
    pub voted_for: i64,
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
    #[prost(uint64, tag = "8")]
    pub next_heartbeat_time: u64,
}

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
        Duration::from_millis(
            ELECTION_TIMEOUT_MIN_TIME + rand::random::<u64>() % ELECTION_TIMEOUT_RANGE,
        )
    }
    pub fn update_heart_beat_timeout(&mut self) {
        self.next_heartbeat_time = system_time_now_epoch() + HEARTBEAT_CHECK_INTERVAL;
    }

    // set check_election_time to now+random_timeout
    pub fn update_election_check_time_out(&mut self) {
        let start = system_time_now_epoch();
        let check_time = start + Self::random_timeout().as_millis() as u64;
        self.time_to_check_election_time_out = check_time;
    }

    pub fn get_role(&self) -> Role {
        match self.role {
            0 => Role::LEADER,
            1 => Role::FOLLOWER,
            2 => Role::CANDIDATOR,
            _ => {
                panic!("get role is not supported")
            }
        }
    }
    pub fn init_state(&mut self) {
        self.update_election_check_time_out();
        // set role to follower
        self.role = 1;
    }

    fn becomes_leader(&mut self) {
        self.role = 0;
        self.voted_for = -1;
        self.update_heart_beat_timeout();
    }
    fn becomes_candidate(&mut self) {
        self.role = 2;
        self.term += 1;
        self.voted_for = -1;
        self.update_election_check_time_out();
    }
    fn becomes_follower(&mut self, term: u64, vote_for: u64) {
        self.role = 1;
        self.term += term;
        self.voted_for = vote_for as i64;
        self.update_election_check_time_out();
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
    rx: Receiver<RaftEvent>,
    tx: Sender<RaftEvent>,

    vote_record: HashSet<u64>,

    // for test
    request_disabled: bool,
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

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            rx: rx,
            tx: tx,
            request_disabled: false,
            vote_record: HashSet::new(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    // for test
    fn set_state(&mut self, state: State) {
        self.state = state;
    }
    fn main(mut self) {
        // init state
        self.state.init_state();
        // set up timer
        self.set_up_election_timeout_checker();

        loop {
            //
            let res = self.rx.recv();
            info!("receive raft event {:?}", res);
            let state = &self.state;
            match res {
                Err(RecvError) => {
                    info!("raft handler chan receive error {},stop handle", RecvError);
                    return;
                }
                Ok(event) => match event {
                    RaftEvent::LeaderTimeOutCheck(instant) => {
                        self.handle_election_timeout(instant);
                        self.set_up_election_timeout_checker();
                    }
                    RaftEvent::Vote(vote) => {
                        debug!("recv vote event");
                        self.handle_vote(&vote.request);
                    }
                    RaftEvent::VoteReply(reply_result) => match reply_result {
                        Ok(reply) => {
                            self.handle_vote_reply(reply);
                        }
                        Err(err) => {
                            error!("recieved vote reply error {:?}", err);
                        }
                    },
                    RaftEvent::ReadState(reply) => {
                        reply
                            .send(self.get_state())
                            .expect("send read state reply failed");
                    }
                    _ => {
                        info!("main recieved others event,just ignore");
                    }
                },
            }
        }
    }

    fn run(mut self) -> Sender<RaftEvent> {
        self.state.init_state();
        self.persist_state();

        let res = self.tx.clone();
        let _ = thread::spawn(move || self.main());
        res
    }

    fn handle_election_timeout(&mut self, instant: u64) {
        if self.state.time_to_check_election_time_out != instant {
            return;
        }
        info!("{} elclection timeout,start election", self.me);

        //  update state
        self.state.becomes_candidate();
        self.vote_record.clear();

        self.persist_state();
        // send vote
        if self.request_disabled() {
            return;
        }
        self.send_vote(self.state.term);
    }
    fn handle_vote(&mut self, vote: &RequestVoteArgs) -> RequestVoteReply {
        return if vote.term > self.state.term()
            && self.state.get_role() != Role::CANDIDATOR
            && self.state.voted_for < 0
        {
            self.state.becomes_follower(vote.term, vote.peer_id);
            RequestVoteReply {
                peer_id: self.me as u64,
                term: self.state.term,
                vote_granted: false,
            }
        } else {
            RequestVoteReply {
                peer_id: self.me as u64,
                term: self.state.term,
                vote_granted: false,
            }
        };
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term == self.state.term()
            && self.state.get_role() == Role::CANDIDATOR
            && reply.vote_granted
        {
            self.vote_record.insert(reply.peer_id);
            if self.vote_record.len() >= self.peers.len() / 2 + 1 {
                info!("get votes from majority ");
                self.state.becomes_leader();
                self.send_heartbert();
            }
        }
    }

    fn send_heartbert(&mut self) {
        todo!()
    }

    fn set_up_election_timeout_checker(&mut self) {
        let check_time = self.state.time_to_check_election_time_out;
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
            tx.send(RaftEvent::LeaderTimeOutCheck(check_time))
                .expect("send time out check error");
            debug!("send time out check event");
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
        // todo need test
        if data.is_empty() {
            info!("raft persist is empty");
            return;
        }
        let res = labcodec::decode::<State>(data);
        match res {
            Ok(o) => self.state = o,
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
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }

    // for test
    fn disable_requests(&mut self) {
        self.request_disabled = true;
    }
    fn request_disabled(&mut self) -> bool {
        self.request_disabled
    }

    pub fn get_state(&self) -> State {
        self.state.clone()
    }

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
    pub fn is_election_time_out(&self, time: u64) -> bool {
        self.state.time_to_check_election_time_out.eq(&time)
    }

    pub fn persist_state(&mut self) {
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
    since_the_epoch.as_millis() as u64
}

#[cfg(test)]
pub mod my_tests {
    use std::{
        panic, println, process,
        sync::{Arc, Mutex, Once},
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        todo, vec,
    };

    use futures::{
        channel::mpsc::unbounded,
        executor::{block_on, ThreadPool},
    };
    use labrpc::Client;

    use super::{persister::SimplePersister, system_time_now_epoch, Node, Raft};
    use crate::{
        proto::raftpb::RequestVoteReply,
        raft::{Role, State, ELECTION_TIMEOUT_MIN_TIME, ELECTION_TIMEOUT_RANGE},
    };
    use crate::{proto::raftpb::*, raft::HEARTBEAT_CHECK_INTERVAL};

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

    fn create_raft() -> Raft {
        init_logger();
        crash_process_when_any_thread_panic();
        let p = SimplePersister::new();
        let (tx, _) = unbounded();
        Raft::new(vec![], 0, Box::new(p), tx)
    }

    fn create_node() -> Node {
        Node::new(create_raft())
    }
    fn create_client() -> RaftClient {
        use crate::proto::raftpb::*;
        let (sender, rx) = unbounded();
        let c = Client {
            name: String::from("test"),
            sender,
            worker: ThreadPool::new().unwrap(),
            hooks: Arc::new(Mutex::new(None)),
        };
        let client = RaftClient::new(c);
        client
    }
    #[test]
    fn test_update_check_time_out() {
        let mut s = State::default();
        let now = system_time_now_epoch();
        s.update_election_check_time_out();
        let t = s.time_to_check_election_time_out;
        assert!(t - now >= 150);
        assert!(t - now <= 300);
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
    fn test_init_state() {
        let mut s = State::default();
        s.init_state();
        let now = system_time_now_epoch();
        assert!(s.get_role() == Role::FOLLOWER);
        assert!(s.term() == 0);
        assert!(s.time_to_check_election_time_out - now > 140);
        assert!(s.time_to_check_election_time_out - now <= 300);
    }
    #[test]
    fn test_become_leader() {
        let mut s = State::default();
        s.init_state();
        let old_term = s.term();
        s.becomes_leader();

        assert!(s.is_leader());
        assert!(s.term() == old_term);
        let now = system_time_now_epoch();
        assert!(s.next_heartbeat_time - now > HEARTBEAT_CHECK_INTERVAL - 2);
        assert!(s.next_heartbeat_time - now < HEARTBEAT_CHECK_INTERVAL + 2);
    }
    #[test]
    fn test_become_candidate() {
        let mut s = State::default();
        s.init_state();

        let old_term = s.term();

        s.becomes_candidate();
        assert!(s.get_role() == Role::CANDIDATOR);
        assert!(s.term() == old_term + 1);
        let now = system_time_now_epoch();
        assert!(s.time_to_check_election_time_out - now > 100);
        assert!(s.time_to_check_election_time_out - now < 300);
    }
    #[test]
    fn test_become_follower() {
        let mut s = State::default();
        s.init_state();

        let new_term = 3;
        s.becomes_follower(new_term, 1);
        assert!(s.get_role() == Role::FOLLOWER);
        assert!(s.voted_for == 1);

        let now = system_time_now_epoch();
        assert!(s.time_to_check_election_time_out - now > ELECTION_TIMEOUT_MIN_TIME - 5);
        assert!(
            s.time_to_check_election_time_out - now
                < ELECTION_TIMEOUT_MIN_TIME + ELECTION_TIMEOUT_RANGE - 5
        );
    }
    #[test]
    fn test_read_state() {
        let node = create_node();
        let stat = node.get_state();
        assert_eq!(stat.get_role(), Role::FOLLOWER);
    }
    #[test]
    fn test_follow_election() {
        let node = create_node();
        let stat = node.get_state();
        assert_eq!(stat.get_role(), Role::FOLLOWER);
        assert_eq!(stat.term(), 0);
        thread::sleep(Duration::from_millis(350));
        let stat = node.get_state();
        assert_eq!(stat.get_role(), Role::CANDIDATOR);
        assert_eq!(stat.term(), 1);
    }
    #[test]
    fn test_election_win() {
        init_logger();
        crash_process_when_any_thread_panic();
        let p = SimplePersister::new();
        let (tx, _) = unbounded();
        Raft::new(vec![create_client(), create_client()], 0, Box::new(p), tx);
        let mut raft = create_raft();
        raft.disable_requests();
        let mut s = raft.state.clone();
        s.becomes_candidate();
        raft.set_state(s);

        let node = Node::new(raft);
        let tx_lock = node.tx.clone();
        let tx = block_on(tx_lock.lock());
        tx.send(super::RaftEvent::VoteReply(Ok(RequestVoteReply {
            term: 1,
            vote_granted: true,
            peer_id: 0,
        })))
        .unwrap();
        tx.send(super::RaftEvent::VoteReply(Ok(RequestVoteReply {
            term: 1,
            vote_granted: true,
            peer_id: 1,
        })))
        .unwrap();

        let s = node.get_state();

        assert!(s.is_leader())
    }
    #[test]
    fn test_election_fail() {}

    #[test]
    fn test_follow_receive_append() {}
    #[test]
    fn test_candidate_timeout() {}
    #[test]
    fn debug() {
        let a: i64 = 1;
        let b = a as u64;
        println!("b is {}", b);
    }
}
