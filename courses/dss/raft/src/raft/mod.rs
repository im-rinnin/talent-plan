use core::panic;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::channel::oneshot;
use log::debug;
use log::error;
use log::info;
use prost::Message;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem::swap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::spawn;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
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
use crate::proto::raftpb::*;

const HEARTBEAT_CHECK_INTERVAL: u64 = 40;
const ELECTION_TIMEOUT_MIN_TIME: u64 = 150;
const ELECTION_TIMEOUT_RANGE: u64 = 150;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug)]
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

#[derive(Clone, Message)]
pub struct Logs {
    #[prost(uint64, tag = "1")]
    first_entry_index: u64,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<Entry>,
}
impl Logs {
    fn new() -> Self {
        Logs {
            first_entry_index: 0,
            entries: vec![Entry {
                data: vec![],
                term: 0,
            }],
        }
    }

    // tructate to
    fn truncate(&mut self, index: u64) {
        assert!(index >= self.first_entry_index);
        let n = index - self.first_entry_index;
        self.entries.truncate(n as usize);
    }
    fn clear(&mut self) {
        self.entries.clear();
    }

    // discard all entry before index (exclude)
    // return last (index,term) in last discard entry
    fn compact(&mut self, index: u64) -> Option<(u64, u64)> {
        assert!(index > 0);
        if index < self.first_entry_index {
            return None;
        }
        let n = index - self.first_entry_index;
        if n > self.entries.len() as u64 {
            warn!(
                "index is {} ,more than log len {}",
                index,
                self.entries.len()
            );
            return None;
        }
        let last_enry = self.entries.last().unwrap();
        let mut data = vec![];
        labcodec::encode(last_enry, &mut data).unwrap();

        let mut rest = self.entries.split_off(n as usize);
        swap(&mut rest, &mut self.entries);
        let last_index = self.first_entry_index + rest.len() as u64 - 1;
        let last_term = rest.last().unwrap().term;
        self.first_entry_index = last_index + 1;
        Some((last_index, last_term))
    }

    fn push(&mut self, entry: Entry) {
        self.entries.push(entry)
    }

    // get any index of entry which term is
    fn index_of_term(&mut self, term: u64) -> Option<u64> {
        let res = self.entries.binary_search_by(|e| e.term.cmp(&term));
        if let Ok(n) = res {
            Some(n as u64 + self.first_entry_index)
        } else {
            None
        }
    }

    fn set_first_index(&mut self, index: u64) {
        self.first_entry_index = index;
    }
    fn first_index(&mut self) -> Option<u64> {
        if !self.entries.is_empty() {
            Some(self.first_entry_index)
        } else {
            None
        }
    }

    fn get(&self, index: u64) -> Option<&Entry> {
        if index < self.first_entry_index
            || index >= self.first_entry_index + self.entries.len() as u64
        {
            return None;
        }
        self.entries.get((index - self.first_entry_index) as usize)
    }
    // for test
    fn entry_in_log_len(&self) -> u64 {
        self.entries.len() as u64
    }

    fn last_entry_index_term(&self) -> Option<(u64, u64)> {
        if !self.entries.is_empty() {
            return Some((
                self.entries.len() as u64 + self.first_entry_index - 1,
                self.entries.last().unwrap().term,
            ));
        } else {
            None
        }
    }

    // include entry in snapshot
    fn len(&self) -> u64 {
        self.first_entry_index + self.entries.len() as u64
    }
}

#[derive(Clone, Message)]
pub struct Snapshot {
    // last entry term
    #[prost(uint64, tag = "1")]
    term: u64,
    // last entry index
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(bytes, tag = "3")]
    data: Vec<u8>,
}

#[derive(Clone, Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, required, tag = "3")]
    pub logs: Logs,
}

#[derive(Clone, Debug)]
pub struct VolatileState {
    role: Role,
    commit_index: u64,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
    vote_record: HashSet<u64>,
    last_send_append_time: u64,
    // set to current_time+random_time[150,300)
    // update when receive any messages from valid leader/candidate
    time_to_check_election_time_out: u64,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            role: Role::FOLLOWER,
            commit_index: Default::default(),
            next_index: Default::default(),
            match_index: Default::default(),
            time_to_check_election_time_out: Default::default(),
            last_send_append_time: 0,
            vote_record: Default::default(),
        }
    }
}

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    _me: usize,
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

#[derive(Debug)]
struct VoteRequestEvent {
    request: RequestVoteArgs,
    reply: oneshot::Sender<RequestVoteReply>,
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
    // (checkout_time,term)
    CheckElectionTimeOut(u64, u64),
    HeartBeatTimeOut(),
    Append(AppendArgs, oneshot::Sender<AppendReply>),
    AppendReply {
        reply: AppendReply,
        peer_id: u64,
        // if append success, leader should update match index to this
        match_index: u64,
        // prev entry index in append, for check reorder reply
        prev_index: u64,
    },
    VoteReply(RequestVoteReply),
    Vote(VoteRequestEvent),

    ReadState(Sender<State>),
    ClientCommand {
        data: Vec<u8>,
        reply: Sender<ClientCommandReply>,
    },
    Snapshot {
        index: u64,
        data: Vec<u8>,
    },
    InstallSnapshotRpc {
        data: Vec<u8>,
        last_entry_index: u64,
        last_entry_term: u64,
        term: u64,
    },
    InstallSnapshot {
        data: Vec<u8>,
        index: u64,
        term: u64,
        reply: crossbeam_channel::Sender<(bool, u64)>,
    },
    InstallSnapshotReply {
        peer_id: u64,
        term: u64,
        success: bool,
        last_index: u64,
    },
    Stop,
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
    snapshot: Option<Snapshot>,
    rx: Receiver<RaftEvent>,
    tx: Sender<RaftEvent>,
    // (checkout_time,term) use term to validate if it is staled
    election_time_out_tx: Sender<(u64, u64)>,
    apply_tx: UnboundedSender<ApplyMsg>,
    disable_timer: bool,
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
        let snapshot_data = persister.snapshot();
        let (tx, rx) = crossbeam_channel::unbounded::<RaftEvent>();

        let mut p = PersistentState::default();
        p.logs.push(Entry {
            data: vec![],
            term: 0,
        });
        let (election_tx, election_rx) = crossbeam_channel::unbounded();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            persistent_state: p,
            volatile_state: VolatileState::default(),
            snapshot: None,
            rx,
            tx,
            apply_tx: apply_ch,
            disable_timer: false,
            election_time_out_tx: election_tx,
        };

        rf.set_up_election_timeout_checker(election_rx);

        // initialize from state persisted before a crash
        rf.restore(&raft_state, &snapshot_data);
        // update commite index to last index in snapshot
        if let Some(s) = &rf.snapshot {
            rf.volatile_state.commit_index = s.index;
        }

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
        self.election_time_out_tx
            .send((check_time, self.persistent_state.term))
            .unwrap();
        info!(
            "{} set election timeout check time to {}",
            self.me, check_time
        );
    }

    fn main(mut self) {
        loop {
            let res = self.rx.recv();
            info!("{} receive raft event {:?}", self.me, res);
            match res {
                Err(e) => {
                    info!(
                        "{} raft handler chan receive error {:?},stop handle",
                        self.me, e
                    );
                    return;
                }
                Ok(event) => match event {
                    RaftEvent::CheckElectionTimeOut(instant, term) => {
                        self.handle_election_timeout(instant, term);
                    }
                    RaftEvent::HeartBeatTimeOut() => {
                        self.handle_heartbeat_event();
                    }
                    RaftEvent::Vote(vote) => {
                        info!("{} recv vote event {:?}", self.me, &vote.request);
                        let reply = self.handle_vote(&vote.request);
                        vote.reply.send(reply).expect("send vote reply error");
                    }
                    RaftEvent::VoteReply(reply_result) => {
                        info!("{} recv vote reply {:?}", self.me, reply_result);
                        self.handle_vote_reply(reply_result);
                    }
                    RaftEvent::Append(request, reply) => self.handle_append(request, reply),
                    RaftEvent::AppendReply {
                        reply,
                        peer_id,
                        match_index,
                        prev_index,
                    } => {
                        self.handle_append_reply(reply, peer_id, match_index, prev_index);
                    }
                    RaftEvent::ReadState(reply) => {
                        reply
                            .send(State {
                                _me: self.me,
                                p_state: self.get_persitent_state(),
                                v_state: self.get_volatitle_state(),
                            })
                            .expect("send read state reply failed");
                    }
                    RaftEvent::ClientCommand { data, reply } => self.handle_command(data, reply),
                    RaftEvent::Snapshot { index, data } => self.handle_snapshot(index, data),
                    RaftEvent::InstallSnapshot {
                        data,
                        index,
                        term,
                        reply,
                    } => self.handle_install_snapshot(data, index, term, reply),
                    RaftEvent::InstallSnapshotRpc {
                        data,
                        last_entry_index,
                        last_entry_term,
                        term,
                    } => self.handle_install_snapshot_rpc(
                        term,
                        data,
                        last_entry_term,
                        last_entry_index,
                    ),
                    RaftEvent::InstallSnapshotReply {
                        term,
                        success,
                        peer_id,
                        last_index,
                    } => self.handle_install_snapshot_reply(peer_id, success, term, last_index),
                    RaftEvent::Stop => {
                        info!("{} rece stop event,return", self.me);
                        return;
                    }
                },
            }
        }
    }

    fn handle_install_snapshot_rpc(
        &mut self,
        term: u64,
        data: Vec<u8>,
        last_entry_term: u64,
        last_entry_index: u64,
    ) {
        if term < self.persistent_state.term {
            info!(
                "{} recv install snapshot rpc ,term is {} less than me {},ignore",
                self.me, term, self.persistent_state.term
            );
            return;
        }
        self.becomes_follower(term, None);
        info!("{} send apply msg snapshot ", self.me);
        let msg = ApplyMsg::Snapshot {
            data,
            term: last_entry_term,
            index: last_entry_index,
        };
        let res = self.apply_tx.unbounded_send(msg);
        if res.is_err() {
            warn!("send apply msg error {:?}", res);
        }
        info!("{} send apply msg snapshot successful", self.me);
    }
    fn run_with_timer(self, enable_timer: bool) -> Sender<RaftEvent> {
        self.run_with_timer_role(enable_timer, Role::FOLLOWER)
    }
    // disable timer for test
    fn run_with_timer_role(mut self, enable_timer: bool, role: Role) -> Sender<RaftEvent> {
        let res = self.tx.clone();
        if !enable_timer {
            self.disable_timer = true;
        } else {
            self.set_up_heartbeat_timer();
        }
        match role {
            Role::CANDIDATOR => self.becomes_candidate(),
            Role::LEADER => self.become_leader(),
            Role::FOLLOWER => self.becomes_follower(self.persistent_state.term, None),
        };
        let _ = thread::spawn(move || self.main());
        res
    }

    fn run(self) -> Sender<RaftEvent> {
        self.run_with_timer(true)
    }

    fn becomes_candidate(&mut self) {
        info!("{} become candidate", self.me);
        self.persistent_state.term += 1;
        self.volatile_state.role = Role::CANDIDATOR;
        self.volatile_state.vote_record.clear();
        self.volatile_state.vote_record.insert(self.me as u64);

        self.update_election_check_time_out();

        self.save_persist_state();
    }

    fn becomes_follower(&mut self, term: u64, vote_for: Option<u64>) {
        assert!(term >= self.persistent_state.term);
        // change role to follower
        self.volatile_state.role = Role::FOLLOWER;
        // update term
        self.persistent_state.term = term;
        self.persistent_state.voted_for = vote_for;
        // update election timeout
        self.update_election_check_time_out();
        self.save_persist_state();
    }

    fn handle_election_timeout(&mut self, instant: u64, term: u64) {
        if term != self.persistent_state.term {
            return;
        }
        if self.volatile_state.role == Role::LEADER {
            return;
        }
        if self.volatile_state.time_to_check_election_time_out != instant {
            info!("{} elclection timeout check pass", self.me);
            return;
        }
        info!("{} elclection timeout,start election", self.me);

        //  update state
        self.becomes_candidate();

        self.send_vote(self.persistent_state.term);
    }
    fn handle_vote(&mut self, vote: &RequestVoteArgs) -> RequestVoteReply {
        let mut res = RequestVoteReply {
            peer_id: self.me as u64,
            term: self.persistent_state.term,
            vote_granted: false,
        };
        match vote.term.cmp(&self.persistent_state.term) {
            // term <me refuse
            Ordering::Less => {
                info!(
                    "{} receive vote {:?},but current term is {},refuse it",
                    self.me, vote, self.persistent_state.term
                );
                res.vote_granted = false;
                res
            }
            Ordering::Greater => {
                let log_compare_res =
                    self.compare_vote_last_entry(vote.last_log_index, vote.last_log_term);
                if log_compare_res {
                    res.term = vote.term;
                    res.vote_granted = true;
                    self.becomes_follower(vote.term, Some(vote.peer_id));
                    info!("{} accept vote {:?},becomte follower", self.me, vote);
                    res
                } else {
                    info!(
                        "{} compare logs with vote last entry {:?} false, refuse vote ",
                        self.me, vote
                    );
                    self.becomes_follower(vote.term, None);
                    res.vote_granted = false;
                    res
                }
            }
            Ordering::Equal => {
                res.vote_granted = false;
                if let Some(p) = self.persistent_state.voted_for {
                    // Some(p) => {
                    if p == vote.peer_id {
                        res.vote_granted = true;
                        self.update_election_check_time_out();
                    }
                    // }
                    // None => {}
                }
                res
            }
        }
    }

    fn compare_vote_last_entry(
        &self,
        vote_last_entry_index: u64,
        vote_last_entry_term: u64,
    ) -> bool {
        let (last_entry_index, last_entry_term) = self.last_entry_index_term();

        info!(
            "{} last entry index is {},last entry term is {}",
            self.me, last_entry_index, last_entry_term
        );

        match last_entry_term.cmp(&vote_last_entry_term) {
            Ordering::Equal => match last_entry_index.cmp(&vote_last_entry_index) {
                Ordering::Less => true,
                Ordering::Equal => true,
                Ordering::Greater => false,
            },
            Ordering::Greater => false,
            Ordering::Less => true,
        }
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term > self.persistent_state.term {
            info!(
                "{} receive vote reply, term is {} bigger than mine {},become follower",
                self.me, reply.term, self.persistent_state.term
            );
            self.becomes_follower(reply.term, None);
            return;
        }
        if reply.term == self.persistent_state.term
            && self.volatile_state.role == Role::CANDIDATOR
            && reply.vote_granted
            && self
                .volatile_state
                .vote_record
                .get(&reply.peer_id)
                .is_none()
        {
            info!("{} vote accept add one from {}", self.me, reply.peer_id);
            self.volatile_state.vote_record.insert(reply.peer_id);
            if self.volatile_state.vote_record.len() > self.peers.len() / 2 {
                self.become_leader();
                self.sync_all_other_raft();
            }
        }
    }
    fn handle_append_reply(
        &mut self,
        reply: AppendReply,
        peer_id: u64,
        match_index: u64,
        prev_index: u64,
    ) {
        // validate reply
        if self.volatile_state.role != Role::LEADER {
            info!(
                "{} receive append reply but current role is not leader,ignore",
                self.me
            );
            return;
        }
        if reply.term < self.persistent_state.term {
            info!(
                "{} receive append, term {} is less than me:{} ignore",
                self.me, reply.term, self.persistent_state.term
            );
            return;
        }
        if reply.term > self.persistent_state.term {
            info!(
                "{} accept append reply ,term is bigger than me ,{} become follower",
                self.me, self.me
            );
            self.becomes_follower(reply.term, None);
            return;
        }
        // reply.term == self.persistent_state.term
        // check if reply is stale
        let next_index = self.volatile_state.next_index.get(&peer_id).unwrap();
        if *next_index - 1 != prev_index {
            warn!(
                "{} receive a reorder reply, prev index is {},current next_index is {},ignore it ",
                self.me, prev_index, *next_index
            );
            return;
        }
        //
        if reply.success {
            self.handle_append_success(match_index, &peer_id);
        } else {
            let next_index = self.volatile_state.next_index.get_mut(&peer_id).unwrap();
            let conflict_entry_term = reply.conflict_entry_term;
            if reply.conflict_entry_term == 0 {
                // not exits,next_index=reply.logs_len
                assert!(reply.logs_len > 0);
                *next_index = reply.logs_len;
            } else {
                // check if conflict term exit in leader log
                let same_term_index_res = self
                    .persistent_state
                    .logs
                    .index_of_term(conflict_entry_term);
                // .persistent_state
                // .logs
                // .binary_search_by(|a| a.term.cmp(&conflict_entry_term));
                match same_term_index_res {
                    // term found in leader
                    Some(mut index) => {
                        // not same as prev term,next_index=last entry index in term
                        let last_index = {
                            loop {
                                let term = self.persistent_state.logs.get(index + 1).unwrap().term;
                                if term != conflict_entry_term {
                                    break;
                                }
                                index += 1;
                            }
                            index
                        };
                        *next_index = last_index;
                    }
                    // term but not found in leader,next_index=reply.conflict_entry_term_first_index
                    None => {
                        assert!(reply.conflict_entry_term_first_index > 0);
                        assert!(reply.conflict_entry_term_first_index < *next_index);
                        *next_index = reply.conflict_entry_term_first_index;
                    }
                }
            }
            // resend append
            self.sync_raft(peer_id as usize);
            // send snapshot
        }
    }

    fn handle_append_success(&mut self, match_index: u64, peer_id: &u64) {
        let next_index = self.volatile_state.next_index.get_mut(peer_id).unwrap();
        // update next_index
        *next_index = match_index + 1;
        let peer_match_index = self.volatile_state.match_index.get_mut(peer_id).unwrap();
        *peer_match_index = match_index;
        //  update commit index
        // add this node log last index
        let mut match_indexs = vec![self.persistent_state.logs.len() - 1];
        // add other raft match index
        for (id, index) in &self.volatile_state.match_index {
            if (*id as usize) == self.me {
                continue;
            }
            match_indexs.push(*index);
        }
        match_indexs.sort();
        match_indexs.reverse();
        let n = match_indexs.get(match_indexs.len() / 2).unwrap();
        info!(
            "{} current major match index is {} ,match indexs is {:?},current commit index is {}",
            self.me, *n, match_indexs, self.volatile_state.commit_index
        );

        if *n > self.volatile_state.commit_index
            && self.index_term(*n as usize).expect("fail to get term in")
                == self.persistent_state.term
        {
            info!("{} update committed index to {}", self.me, *n);
            for index in (self.volatile_state.commit_index + 1) as usize..(*n + 1) as usize {
                let entry = self
                    .persistent_state
                    .logs
                    .get(index as u64)
                    .expect("index  not commit,should not be compact to snapshot");
                let apply_msg = ApplyMsg::Command {
                    data: entry.data.clone(),
                    index: index as u64,
                };
                info!(
                        "{} leader apply entry {:?} to index {} for apped apply,data is {:?}, data len is {}",
                        self.me, apply_msg, index, entry.data,entry.data.len());
                let res = self.apply_tx.unbounded_send(apply_msg);
                if res.is_err() {
                    info!("{} send apply msg error {:?}", self.me, res)
                }
            }
            self.volatile_state.commit_index = *n;
        }
    }

    fn handle_append(&mut self, args: AppendArgs, reply_ch: oneshot::Sender<AppendReply>) {
        // validate term
        let reply = if args.term < self.persistent_state.term {
            info!(
                "{} term {} is less than mine term{},refuse append",
                self.me, args.term, self.persistent_state.term
            );
            AppendReply {
                term: self.persistent_state.term,
                success: false,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: self.persistent_state.logs.len(),
            }
        } else {
            self.handle_valid_append(args)
        };

        info!("{} send append reply {:?}", self.me, reply);
        reply_ch
            .send(reply)
            .expect("send append reply to chan failed");
    }

    fn handle_valid_append(&mut self, args: AppendArgs) -> AppendReply {
        info!(
            "{} accept append from {},term is eq or more than me, {} become follower",
            self.me, args.leader_id, self.me
        );
        self.becomes_follower(args.term, None);

        // append/tructe log if match, or refuse log change
        let max_index = self.last_log_index() as u64;
        if max_index < args.prev_log_index {
            info!("{} prev entry not found, refuse append", self.me);
            return AppendReply {
                term: self.persistent_state.term,
                success: false,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: self.persistent_state.logs.len(),
            };
        }
        let prev_index_term_res = self.index_term(args.prev_log_index as usize);
        if prev_index_term_res.is_none() {
            info!("{} prev entry not found, refuse append", self.me);
            return AppendReply {
                term: self.persistent_state.term,
                success: false,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: self.persistent_state.logs.len(),
            };
        }
        let prev_index_term = prev_index_term_res.unwrap();
        if prev_index_term != args.prev_log_term {
            info!("prev entry not match,refuse append,index is {}, entry term is {}, args prev log term is {}",args.prev_log_index, prev_index_term,args.term);
            let conflict_entry_term = prev_index_term;
            let conflict_entry_term_first_index =
                self.term_first_index(args.prev_log_index as usize);

            return AppendReply {
                term: self.persistent_state.term,
                success: false,
                conflict_entry_term_first_index,
                conflict_entry_term,
                logs_len: self.persistent_state.logs.len(),
            };
        }
        // check first entry conflict
        let mut log_index = args.prev_log_index + 1;
        let mut index_in_append_args = 0;
        while log_index <= max_index && index_in_append_args < args.entrys.len() {
            let term_res = self.index_term(log_index as usize);
            match term_res {
                Some(term) => {
                    let append_term =
                        labcodec::decode::<Entry>(args.entrys.get(index_in_append_args).unwrap())
                            .unwrap()
                            .term;
                    if term != append_term {
                        // discard entry start from conflict entry
                        self.persistent_state.logs.truncate(log_index);
                        break;
                    }
                }
                None => {
                    info!("{} log index is {},max index is {},arg prev log index is {},log is {:?},snapshot is {:?}",self.me,log_index,max_index,args.prev_log_index,self.persistent_state.logs,self.snapshot);
                    // already in snapshot,skip check
                    assert!(self.snapshot.as_ref().unwrap().index > log_index);
                }
            }
            log_index += 1;
            index_in_append_args += 1;
        }
        // append entry from conflict entry or from rest append entry
        for i in index_in_append_args..args.entrys.len() {
            let entry = labcodec::decode::<Entry>(args.entrys.get(i).unwrap()).unwrap();
            self.persistent_state.logs.push(entry);
        }

        if self.volatile_state.commit_index < args.leader_commit
            && self.last_log_index() as u64 > self.volatile_state.commit_index
        {
            let end = min(self.last_log_index(), args.leader_commit as usize) as u64;
            for i in self.volatile_state.commit_index + 1..end + 1 {
                let entry_res = self.persistent_state.logs.get(i);
                let entry = entry_res.unwrap();
                let apply_msg = ApplyMsg::Command {
                    data: entry.data.clone(),
                    index: i,
                };

                info!(
                        "{} apply entry {:?} to index {},accept append from {},data is {:?} data len is {}",
                        self.me, apply_msg, i, args.leader_id, entry.data,entry.data.len()
                    );
                let res = self.apply_tx.unbounded_send(apply_msg);
                if res.is_err() {
                    warn!("send apply msg error {:?}", res);
                }
            }
            self.volatile_state.commit_index = end;
        }

        self.save_persist_state();
        AppendReply {
            term: self.persistent_state.term,
            success: true,
            conflict_entry_term_first_index: 0,
            conflict_entry_term: 0,
            logs_len: self.persistent_state.logs.len(),
        }
    }
    fn handle_command(&mut self, data: Vec<u8>, reply: Sender<ClientCommandReply>) {
        if self.volatile_state.role != Role::LEADER {
            reply
                .send(ClientCommandReply {
                    success: false,
                    index: 0,
                    term: 0,
                })
                .expect("send command reply error");
            return;
        }
        let entry = Entry {
            data,
            term: self.persistent_state.term,
        };
        self.persistent_state.logs.push(entry);
        self.volatile_state.last_send_append_time = system_time_now_epoch();
        self.save_persist_state();
        self.sync_all_other_raft();

        reply
            .send(ClientCommandReply {
                success: true,
                index: (self.persistent_state.logs.len() - 1),
                term: self.persistent_state.term,
            })
            .expect("send command reply error");
    }

    fn handle_install_snapshot_reply(
        &mut self,
        peer_id: u64,
        success: bool,
        term: u64,
        last_index: u64,
    ) {
        if term < self.persistent_state.term {
            warn!(
                "recevie a stale install snapshot reply,peer is {} term is {},ignore",
                peer_id, term
            );
            return;
        }
        // check term,become follower if is greater than me
        if term > self.persistent_state.term {
            self.becomes_follower(term, None);
            return;
        }
        if !success {
            // resend later
            warn!("{} send install snapshot to {} fail", self.me, peer_id);
            return;
        }
        let next_index = self.volatile_state.next_index.get_mut(&peer_id).unwrap();
        if *next_index > last_index {
            warn!("{} receive install snapshot ,but last_index in snapshot return is {}, is less than current next_index {},ignore ", self.me, last_index,*next_index);
            return;
        }
        *next_index = last_index + 1;
        let match_index = self.volatile_state.match_index.get_mut(&peer_id).unwrap();
        *match_index = last_index;
        // update next index
        self.save_persist_state();
        // sync more log
        self.sync_raft(peer_id as usize);
    }

    fn handle_install_snapshot(
        &mut self,
        data: Vec<u8>,
        index: u64,
        term: u64,
        reply: crossbeam_channel::Sender<(bool, u64)>,
    ) {
        let logs = &mut self.persistent_state.logs;
        // remove all log entites in snaphsot
        match logs.get(index) {
            Some(e) => {
                if e.term == term {
                    logs.compact(index + 1);
                } else {
                    // discard all
                    logs.clear();
                }
            }
            None => {
                logs.clear();
            }
        }
        self.snapshot = Some(Snapshot { term, index, data });
        self.volatile_state.commit_index = index;
        logs.set_first_index(index + 1);

        // change snapshot,save
        self.save_persist_state_and_snapshot();
        // reply
        reply.send((true, self.persistent_state.term)).unwrap();
        info!(
            "{} install snapshot index is {}, term is {} ",
            self.me, index, term
        );
    }

    fn handle_snapshot(&mut self, index: u64, data: Vec<u8>) {
        // compact log
        let term = self
            .persistent_state
            .logs
            .compact(index + 1)
            .expect("fail to log compact")
            .1;
        let s = Snapshot { term, index, data };
        assert_eq!(self.persistent_state.logs.first_entry_index, s.index + 1);
        // update snapshot
        // save state and snapshot
        self.snapshot = Some(s);
        self.save_persist_state_and_snapshot();
    }

    fn handle_heartbeat_event(&mut self) {
        if self.volatile_state.role != Role::LEADER {
            return;
        }
        assert!(self.volatile_state.role == Role::LEADER);
        // check last append send
        let now = system_time_now_epoch();
        if now - self.volatile_state.last_send_append_time >= HEARTBEAT_CHECK_INTERVAL {
            info!("{} heart beat timeout, send append to all node", self.me);
            self.sync_all_other_raft();
            self.volatile_state.last_send_append_time = now;
        }
    }

    fn become_leader(&mut self) {
        info!("{} become leader get votes from majority", self.me);
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
        self.save_persist_state();
    }
    fn set_up_heartbeat_timer(&mut self) {
        if self.disable_timer {
            return;
        }
        // set up a timer
        // send a heartbeat check event
        // check if need to send a heartbeat in constant time interval
        let tx = self.tx.clone();
        spawn(move || loop {
            thread::sleep(Duration::from_millis(HEARTBEAT_CHECK_INTERVAL));
            let res = tx.send(RaftEvent::HeartBeatTimeOut());
            if res.is_err() {
                info!("send heartbeat check event error,exit");
                return;
            }
        });
    }

    fn set_up_election_timeout_checker(&mut self, check_time_rx: Receiver<(u64, u64)>) {
        if self.disable_timer {
            return;
        }

        let tx = self.tx.clone();
        let _join = spawn(move || {
            loop {
                let mut check_time;
                let mut term;
                // wait for rx
                let res = check_time_rx.recv();
                match res {
                    Ok((c, t)) => {
                        check_time = c;
                        term = t;
                    }
                    Err(e) => {
                        info!("election recv error{:?},return", e);
                        break;
                    }
                }
                // get all from rx , use last one
                while !check_time_rx.is_empty() {
                    let res = check_time_rx.try_recv();
                    match res {
                        Ok((c, t)) => {
                            check_time = c;
                            term = t;
                        }
                        Err(e) => {
                            info!("election recv error{:?},return", e);
                            break;
                        }
                    }
                }
                // sleep and send event

                let now = system_time_now_epoch();
                let duration = if check_time > now {
                    check_time - now
                } else {
                    0
                };

                info!(
                    " set next time out check after {} ms in {}",
                    duration, check_time
                );
                thread::sleep(Duration::from_millis(duration));
                let res = tx.send(RaftEvent::CheckElectionTimeOut(check_time, term));
                if res.is_err() {
                    info!("send time out check error");
                    return;
                }
            }
        });
    }
    fn index_term(&self, index: usize) -> Option<u64> {
        let entry = &self.persistent_state.logs.get(index as u64);
        match entry {
            Some(e) => Some(e.term),
            None => {
                if let Some(s) = &self.snapshot {
                    if s.index == index as u64 {
                        return Some(s.term);
                    }
                }
                None
            }
        }
    }

    fn last_entry_index_term(&self) -> (u64, u64) {
        if let Some((i, t)) = self.persistent_state.logs.last_entry_index_term() {
            (i, t)
        } else {
            (
                self.snapshot.as_ref().unwrap().index,
                self.snapshot.as_ref().unwrap().term,
            )
        }
    }

    fn term_first_index(&self, entry_index: usize) -> u64 {
        if entry_index == 0 {
            return 0;
        }
        let mut res = entry_index;
        let entry_term = self.index_term(res);
        loop {
            assert!(res > 0);
            res -= 1;
            let term = self.index_term(res);
            if term != entry_term {
                return (res + 1) as u64;
            }
        }
    }

    fn sync_all_other_raft(&mut self) {
        for peer_id in 0..self.peers.len() {
            if peer_id == self.me {
                continue;
            }
            self.sync_raft(peer_id);
        }
    }

    fn sync_raft(&mut self, peer_id: usize) {
        let next_index = *self
            .volatile_state
            .next_index
            .get(&(peer_id as u64))
            .expect("get match index fail");

        let first_index_res = self.persistent_state.logs.first_index();
        if let Some(first_index) = first_index_res {
            if first_index <= next_index {
                self.send_append_to(peer_id as u64, next_index);
                return;
            }
        }
        assert!(self.snapshot.is_some());
        self.send_install_snapshot(peer_id as u64);
    }

    fn send_install_snapshot(&self, peer_id: u64) {
        let s = self.snapshot.as_ref().unwrap();
        let msg = InstallSnapshotRpc {
            data: s.data.clone(),
            leader_id: self.me as u64,
            last_include_index: s.index,
            last_include_term: s.term,
            term: self.persistent_state.term,
        };
        info!("{} send install snapshot {:?} to {}", self.me, msg, peer_id);
        let c = self.peers.get(peer_id as usize).unwrap();
        let c_clone = c.clone();
        let tx = self.tx.clone();
        c.spawn(async move {
            let last_index = msg.last_include_index;
            let res = c_clone.install_snapshot(&msg).await;
            match res {
                Err(e) => {
                    warn!("send install snapshot request failed {:?} ", e);
                }
                Ok(reply) => {
                    let res = tx.send(RaftEvent::InstallSnapshotReply {
                        peer_id,
                        term: reply.term,
                        success: reply.success,
                        last_index,
                    });
                    if res.is_err() {
                        warn!("send snapshot request failed {:?}", res);
                    }
                }
            }
        });
    }

    fn send_append_to(&self, peer_id: u64, next_index: u64) {
        let c = self.peers.get(peer_id as usize).unwrap();

        let mut entrys = vec![];
        for i in next_index..self.persistent_state.logs.len() {
            let mut data = vec![];
            let e_res = self.persistent_state.logs.get(i);
            if e_res.is_none() {
                error!(
                    " {} index is {},log len is{}",
                    self.me,
                    i,
                    self.persistent_state.logs.len()
                );
            }
            let entry = e_res.unwrap();
            labcodec::encode(entry, &mut data).expect("encode entry fail");
            entrys.push(data);
        }
        let prev_log_term = match self.index_term((next_index - 1) as usize) {
            Some(i) => i,
            None => {
                assert_eq!(
                    next_index,
                    self.snapshot.as_ref().unwrap().index + 1,
                    "{} next index is {}",
                    self.me,
                    next_index
                );
                self.snapshot.as_ref().unwrap().term
            }
        };

        let append_request = AppendArgs {
            leader_id: self.me as u64,
            term: self.persistent_state.term,
            prev_log_index: (next_index - 1),
            prev_log_term,
            leader_commit: self.volatile_state.commit_index,
            entrys,
        };
        info!(
            "{} send append request {:?} to peer {}",
            self.me, append_request, peer_id
        );
        let c_clone = c.clone();
        let tx = self.tx.clone();
        c.spawn(async move {
            let prev_log_index = append_request.prev_log_index;
            let res = c_clone.append(&append_request).await;
            match res {
                Err(e) => {
                    warn!("send heartbeat append request failed {:?} ", e);
                }
                Ok(reply) => {
                    let match_index =
                        append_request.prev_log_index + append_request.entrys.len() as u64;
                    let res = tx.send(RaftEvent::AppendReply {
                        reply,
                        peer_id,
                        match_index,
                        prev_index: prev_log_index,
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
            term,
            last_log_index: self.last_log_index() as u64,
            last_log_term: self.last_log_term(),
        };
        for (peer_id, _) in self.peers.iter().enumerate() {
            if peer_id == self.me {
                continue;
            }
            info!(
                "{} send vote request {:?} to peer {}",
                self.me, request, peer_id
            );
            self.send_request_vote(peer_id, request.clone(), self.tx.clone());
        }
    }
    fn restore(&mut self, persiste_data: &[u8], snapshot_data: &[u8]) {
        if persiste_data.is_empty() {
            info!("{} raft persist is empty", self.me);
            return;
        }
        let res = labcodec::decode::<PersistentState>(persiste_data);
        match res {
            Ok(o) => self.persistent_state = o,
            Err(e) => {
                panic!("failed to decode raft pesiste data: {:?}", e)
            }
        }
        if !snapshot_data.is_empty() {
            let res = labcodec::decode::<Snapshot>(snapshot_data).unwrap();
            self.snapshot = Some(res);
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
        let me = self.me;
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            match res {
                Err(e) => {
                    warn!("send vote error {:?}", e);
                }
                Ok(reply) => {
                    let res = result_tx.send(RaftEvent::VoteReply(reply));
                    if res.is_err() {
                        warn!("send resp error {:?}", res);
                    } else {
                        info!("{} send vote {:?} to {:} ok", me, args, server);
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

    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _s = Logs::new();
        _s.entry_in_log_len();
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
        (self.persistent_state.logs.len() - 1) as usize
    }
    // term begin from 0 on start
    pub fn last_log_term(&self) -> u64 {
        let e = self.persistent_state.logs.last_entry_index_term();
        match e {
            None => self.snapshot.as_ref().unwrap().term,
            Some(a) => a.1,
        }
    }
    pub fn is_election_time_out(&self, time: u64) -> bool {
        self.volatile_state
            .time_to_check_election_time_out
            .eq(&time)
    }

    pub fn save_persist_state(&mut self) {
        assert!(self.persistent_state.term >= self.last_log_term());
        info!("{} save persiste state", self.me);
        let mut data = Vec::new();
        self.persistent_state
            .encode(&mut data)
            .expect("encode failed");
        self.persister.save_raft_state(data);
    }
    pub fn save_persist_state_and_snapshot(&mut self) {
        assert!(self.snapshot.is_some());
        assert!(self.persistent_state.term >= self.last_log_term());
        info!("{} save persiste state", self.me);
        let mut data = Vec::new();
        self.persistent_state
            .encode(&mut data)
            .expect("encode failed");
        let mut snapshot_data = vec![];
        self.snapshot
            .as_ref()
            .unwrap()
            .encode(&mut snapshot_data)
            .expect("encode snapshot failed");
        self.persister.save_state_and_snapshot(data, snapshot_data);
    }
}

type InstallSnapshotReply = HashMap<(u64, u64), oneshot::Sender<Result<(bool, u64)>>>;

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
    tx: Arc<Mutex<Sender<RaftEvent>>>, // Your code here.
    id: usize,
    // (index,term)->(success,term)
    snapshot_res: Arc<Mutex<InstallSnapshotReply>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // let handler = RaftHandler::new(raft);
        let id = raft.me;
        let tx = raft.run();
        Node {
            id,
            tx: Arc::new(Mutex::new(tx)),
            snapshot_res: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // for test
    fn _new_disable_timer(raft: Raft) -> Node {
        // Your code here.
        // let handler = RaftHandler::new(raft);
        let id = raft.me;
        let tx = raft.run_with_timer(false);
        Node {
            id,
            tx: Arc::new(Mutex::new(tx)),
            snapshot_res: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // for test
    fn _new_wiht_timer_and_role(raft: Raft, enable_timer: bool, role: Role) -> Node {
        // Your code here.
        // let handler = RaftHandler::new(raft);
        let id = raft.me;
        let tx = raft.run_with_timer_role(enable_timer, role);
        Node {
            id,
            tx: Arc::new(Mutex::new(tx)),
            snapshot_res: Arc::new(Mutex::new(HashMap::new())),
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
        command.encode(&mut data).unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();

        let g = self.tx.lock().expect("lock fail");
        let res = g.send_timeout(
            RaftEvent::ClientCommand { data, reply: tx },
            Duration::from_millis(20),
        );
        if res.is_err() {
            // maybe is stopped
            return Err(Error::NotLeader);
        }

        let receive_res = rx.recv();
        let res = receive_res;
        if let Ok(r) = res {
            if r.success {
                Ok((r.index, r.term))
            } else {
                Err(Error::NotLeader)
            }
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
        let t = self.tx.lock().expect("lock error");
        t.send(event).expect("send read state request error");
    }
    fn get_state(&self) -> State {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.send_event(RaftEvent::ReadState(tx));
        let res = rx.recv().expect("read state error");
        debug!(" get state res is {:?}", res);
        res
    }

    fn _log_state(&self) {
        let s = self.get_state();
        info!(
            "{} log state, v state is {:?},term is {} ,vote for is {:?},log len is {},last entry is {:?}",
            s._me,
            s.v_state,
            s.p_state.term,
            s.p_state.voted_for,
            s.p_state.logs.len(),
            s.p_state.logs.get(s.p_state.logs.len())
        );
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
        self.send_event(RaftEvent::Stop);
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
        info!(
            "{} start cond_install_snapshot term is {},index is {}",
            self.id, last_included_term, last_included_index
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        self.tx
            .lock()
            .unwrap()
            .send(RaftEvent::InstallSnapshot {
                data: Vec::from(snapshot),
                index: last_included_index,
                term: last_included_term,
                reply: tx,
            })
            .unwrap();

        let res = rx.recv();

        let reply_ch = self
            .snapshot_res
            .lock()
            .unwrap()
            .remove(&(last_included_index, last_included_term));

        info!("{} start cond_install_snapshot return", self.id);
        match res {
            Ok(r) => {
                if let Some(ch) = reply_ch {
                    ch.send(Ok(r)).unwrap();
                }
                r.0
            }
            Err(e) => {
                let e = errors::Error::Rpc(labrpc::Error::Other(e.to_string()));
                if let Some(ch) = reply_ch {
                    ch.send(Err(e)).unwrap();
                }
                false
            }
        }
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        self.tx
            .lock()
            .unwrap()
            .send(RaftEvent::Snapshot {
                index,
                data: Vec::from(snapshot),
            })
            .unwrap()
    }

    pub fn add_snapshot_result_ch(
        &self,
        index: u64,
        term: u64,
        tx: oneshot::Sender<Result<(bool, u64)>>,
    ) {
        self.snapshot_res.lock().unwrap().insert((index, term), tx);
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    async fn install_snapshot(
        &self,
        args: InstallSnapshotRpc,
    ) -> labrpc::Result<InstallSnapshotRpcReply> {
        let (tx, rx) = oneshot::channel();
        let e = RaftEvent::InstallSnapshotRpc {
            data: args.data,
            last_entry_index: args.last_include_index,
            last_entry_term: args.last_include_term,
            term: args.term,
        };
        let send_res = self.tx.lock().unwrap().send(e);
        if let Err(e) = send_res {
            warn!("{} send install snapshot event error {:?}", self.id, e);
            return labrpc::Result::Err(labrpc::Error::Other(e.to_string()));
        }
        self.add_snapshot_result_ch(args.last_include_index, args.last_include_term, tx);
        let res = rx.await;
        match res {
            Ok(r) => match r {
                Ok((res, term)) => Ok(InstallSnapshotRpcReply { term, success: res }),
                Err(e) => labrpc::Result::Err(labrpc::Error::Other(e.to_string())),
            },
            Err(e) => {
                warn!("recve install snapshot error {:?}", e);
                labrpc::Result::Err(labrpc::Error::Other(e.to_string()))
            }
        }
    }

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
        {
            let guard = self.tx.lock().unwrap();
            guard
                .send(RaftEvent::Vote(vote_event))
                .expect("send vote request failed");
        }

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
            {
                let guard = self.tx.lock().expect("lock error");
                let res = guard.send(append_event);
                if let Err(e) = res {
                    warn!("send append event error {:?}", e);
                    return labrpc::Result::Err(labrpc::Error::Other(e.to_string()));
                }
            }

            let t = rx.await;
            match t {
                Ok(res) => labrpc::Result::Ok(res),
                // Err(e) => labrpc::Result::Err(labrpc::Error::Other(e.to_string())),
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
        assert_eq, println,
        sync::{Arc, Mutex, Once},
        thread::{self},
        time::Duration,
        vec,
    };

    use futures::{
        channel::{
            mpsc::{unbounded, UnboundedReceiver},
            oneshot::{self},
        },
        executor::{block_on, ThreadPool},
        StreamExt, TryFutureExt,
    };
    use labcodec::Message;
    use labrpc::{Client, Rpc};

    use super::{
        persister::SimplePersister, system_time_now_epoch, ApplyMsg, Entry, Logs, Node, Raft,
    };
    use crate::{
        proto::raftpb::RequestVoteReply,
        raft::{RaftEvent, Role},
    };
    use crate::{proto::raftpb::*, raft::HEARTBEAT_CHECK_INTERVAL};

    fn init_logger() {
        static LOGGER_INIT: Once = Once::new();
        LOGGER_INIT.call_once(|| env_logger::builder().format_timestamp_micros().init());
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

    fn create_raft_with_client_with_apply_ch(
        clients: Vec<RaftClient>,
        me: usize,
    ) -> (Raft, UnboundedReceiver<ApplyMsg>) {
        init_logger();
        // crash_process_when_any_thread_panic();
        let p = SimplePersister::new();
        let (tx, rx) = unbounded();
        (Raft::new(clients, me, Box::new(p), tx), rx)
    }
    fn create_raft_with_client(clients: Vec<RaftClient>, me: usize) -> Raft {
        let (res, _) = create_raft_with_client_with_apply_ch(clients, me);
        res
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
        let n = Node::_new_wiht_timer_and_role(r, false, Role::FOLLOWER);

        let state = n.get_state();
        assert_eq!(state.v_state.role, Role::FOLLOWER);
        assert_eq!(state.p_state.term, 0);
        let time = state.v_state.time_to_check_election_time_out;
        let now = system_time_now_epoch();
        let duration = time - now;
        // wait time change
        thread::sleep(Duration::from_millis(duration + 1));

        // 超时
        n.send_event(RaftEvent::CheckElectionTimeOut(time, 0));
        // assert vote
        let (reply, _) = get_send_rpc::<RequestVoteArgs>(rx);
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

        let r = create_raft_with_client(vec![c], me);
        let n = Node::_new_wiht_timer_and_role(r, false, Role::CANDIDATOR);

        n.send_event(RaftEvent::VoteReply(RequestVoteReply {
            term: new_term,
            vote_granted: true,
            peer_id: 0,
        }));
        let (reply, _) = get_send_rpc::<AppendArgs>(rx);
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

        let r = create_raft_with_client(vec![c], me);
        let n = Node::_new_wiht_timer_and_role(r, true, Role::CANDIDATOR);
        let state = n.get_state();
        assert_eq!(state.v_state.role, Role::CANDIDATOR);
        let old_term = state.p_state.term;

        // sleep after  election timeout
        thread::sleep(Duration::from_millis(300));
        let state = n.get_state();
        let role = state.v_state.role;
        assert_eq!(role, Role::CANDIDATOR);
        assert_eq!(state.p_state.term, old_term + 1);
        let _reply = get_send_rpc::<RequestVoteArgs>(rx);
    }
    //  followe/leader 接受选举
    #[test]
    fn test_follower_accept_election() {
        let (c, _) = create_raft_client("test");
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
        assert!(reply.vote_granted);
        assert_eq!(reply.peer_id as usize, me);
    }
    // [] followe/leader 拒绝选举 已经投票/term
    #[test]
    fn test_election_refuse_already_vote() {
        let (c, _) = create_raft_client("test");
        let me = 3;
        let mut r = create_raft_with_client(vec![c], me);
        // vote for 5
        let _ = r.persistent_state.voted_for.insert(5);
        let n = Node::_new_wiht_timer_and_role(r, false, Role::CANDIDATOR);

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

        let res = block_on(rx).unwrap();
        assert!(!res.vote_granted)
    }

    //  followe/leader 拒绝选举 logs不够新
    #[test]
    pub fn test_election_follower_refuse_log_is_old() {
        let (c, _) = create_raft_client("test");
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
        r.persistent_state.term = 2;

        let n = Node::_new_disable_timer(r);

        let (tx, rx) = oneshot::channel::<RequestVoteReply>();
        n.send_event(RaftEvent::Vote(super::VoteRequestEvent {
            request: RequestVoteArgs {
                peer_id: 4,
                term: 0,
                last_log_index: 2,
                last_log_term: 2,
            },
            reply: tx,
        }));

        let res = block_on(rx).unwrap();
        assert!(!res.vote_granted)
    }

    // 选举失败，其他节点成功
    #[test]
    fn test_election_but_fail() {
        let (c, _) = create_raft_client("test");
        let me = 3;

        let r = create_raft_with_client(vec![c], me);
        let n = Node::_new_wiht_timer_and_role(r, true, Role::CANDIDATOR);
        let state = n.get_state();
        let term = state.p_state.term;
        let old_check_time = state.v_state.time_to_check_election_time_out;

        let (tx, _rx) = oneshot::channel::<AppendReply>();

        n.send_event(RaftEvent::Append(
            AppendArgs {
                leader_id: 0,
                term,
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
        let (c, _) = create_raft_client("test");
        let me = 3;

        let mut r = create_raft_with_client(vec![c], me);
        let term = 1;
        let leader_id = 1;
        r.persistent_state.term = term;
        let n = Node::_new_wiht_timer_and_role(r, false, Role::FOLLOWER);
        let store_data = vec![1, 2, 3];

        // [x] append 接受term
        let e = Entry {
            data: store_data.clone(),
            term,
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
        let (tx, rx) = oneshot::channel();
        n.send_event(RaftEvent::Append(append_request, tx));
        let reply = block_on(async { rx.into_future().await }).unwrap();
        assert!(reply.success);

        let state = n.get_state();
        let log = &state.p_state.logs;

        assert_eq!(log.len(), 2);

        let _ = log.get(1).unwrap();
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

        let (tx, rx) = oneshot::channel();
        n.send_event(RaftEvent::Append(append_request, tx));

        let res = block_on(rx).unwrap();
        assert!(!res.success);
        assert_eq!(res.term, term);

        // 拒绝，prex不符合
        let append_request = AppendArgs {
            leader_id,
            term,
            prev_log_index: 0,
            prev_log_term: 1,
            leader_commit: leader_id,
            entrys: vec![],
        };
        let (tx, rx) = oneshot::channel::<AppendReply>();
        n.send_event(RaftEvent::Append(append_request, tx));

        let res = block_on(rx).unwrap();
        assert!(!res.success);
        assert_eq!(res.term, term);
        // append 截断 follower
        let e = Entry {
            data: store_data.clone(),
            term,
        };
        let mut data = Vec::new();
        labcodec::encode(&e, &mut data).unwrap();
        let entrys = vec![data.clone(), data];

        let append_request = AppendArgs {
            leader_id,
            term,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: leader_id,
            entrys,
        };
        let (tx, rx) = oneshot::channel();
        n.send_event(RaftEvent::Append(append_request, tx));
        let res = block_on(rx).unwrap();
        assert!(res.success);
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
        let _n = Node::_new_wiht_timer_and_role(r, true, Role::LEADER);
        // sleep and check heartbeat

        thread::sleep(Duration::from_millis(HEARTBEAT_CHECK_INTERVAL + 10));
        let (append, _) = get_send_rpc::<AppendArgs>(rx);

        assert_eq!(append.entrys.len(), 0);
        assert_eq!(append.leader_id, 3);
        assert_eq!(append.term, term);
    }
    // append client 请求成功
    #[test]
    fn test_client_command_success() {
        let (c, _) = create_raft_client("test");
        let me = 1;
        let mut r = create_raft_with_client(vec![c], me);
        // leader start in 1
        r.persistent_state.term = 1;
        let node = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);

        let command = MessageForTest { data: 1 };

        let res = node.start(&command);
        println!("err {:?}", res);
        assert!(res.is_ok());
        let r = res.unwrap();
        assert_eq!(r.0, 1);
        assert_eq!(r.1, 1);
    }

    #[test]
    fn test_client_command_to_non_leader() {
        let (c, _) = create_raft_client("test");
        let me = 1;
        let r = create_raft_with_client(vec![c], me);
        let node = Node::_new_wiht_timer_and_role(r, false, Role::CANDIDATOR);

        let command = MessageForTest { data: 1 };

        let res = node.start(&command);
        assert!(res.is_err());
    }
    // [x]append reply 成功leader 更新commit,match_index,next_index,apply entry
    #[test]
    fn test_commited_index_update() {
        let (c1, rx_1) = create_raft_client("test");
        let (c2, _) = create_raft_client("test");
        let me = 3;

        let (mut r, rx_ch) = create_raft_with_client_with_apply_ch(vec![c1, c2], me);
        let term = 1;
        r.persistent_state.term = term;
        let n = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);
        let command = MessageForTest { data: 1 };

        let _ = n.start(&command);
        let state = n.get_state();
        let (append, _) = get_send_rpc::<AppendArgs>(rx_1);
        assert_eq!(append.prev_log_index, 0);
        assert_eq!(append.entrys.len(), 1);
        assert_eq!(state.v_state.commit_index, 0);
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term,
                success: true,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: 0,
            },
            peer_id: 0,
            match_index: 1,
            prev_index: 0,
        });
        let state = n.get_state();
        assert_eq!(state.v_state.commit_index, 1);
        assert_eq!(*state.v_state.next_index.get(&0).unwrap(), 2);
        assert_eq!(*state.v_state.match_index.get(&0).unwrap(), 1);

        let (res, _) = block_on(async { rx_ch.into_future().await });
        let msg = res.unwrap();
        if let ApplyMsg::Command { data, index } = msg {
            assert_eq!(index, 1);
            let data = labcodec::decode::<MessageForTest>(&data).unwrap();
            assert_eq!(data.data, 1);
            assert_eq!(index, 1);
        }
    }
    // 只对当前term的entry进行commit，之前term的entry，即使数量达到要求，也不进行commit
    #[test]
    fn test_only_update_commite_for_current_term_entry() {
        let (c1, _) = create_raft_client("test");
        let me = 2;
        let (mut r, _) = create_raft_with_client_with_apply_ch(vec![c1], me);
        let term = 3;
        r.persistent_state.term = term;
        let logs = &mut r.persistent_state.logs;

        logs.push(Entry {
            data: vec![1],
            term: 2,
        });

        logs.push(Entry {
            data: vec![1],
            term: 3,
        });

        let n = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);

        // index 2 unmatch
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term: 3,
                success: false,
                conflict_entry_term_first_index: 1,
                conflict_entry_term: 1,
                logs_len: 3,
            },
            peer_id: 0,
            match_index: 0,
            prev_index: 2,
        });

        // index 1 unmatch
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term: 3,
                success: false,
                conflict_entry_term_first_index: 1,
                conflict_entry_term: 1,
                logs_len: 3,
            },
            peer_id: 0,
            match_index: 0,
            prev_index: 1,
        });
        let s = n.get_state();
        assert_eq!(*s.v_state.next_index.get(&0).unwrap(), 1);

        // index 0 match
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term: 3,
                success: true,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: 0,
            },
            peer_id: 0,
            match_index: 1,
            prev_index: 0,
        });

        let s = n.get_state();
        assert_eq!(s.p_state.logs.len(), 3);
        assert_eq!(s.v_state.commit_index, 0);
        assert_eq!(*s.v_state.next_index.get(&0).unwrap(), 2);

        // index 0 match
        n.send_event(RaftEvent::AppendReply {
            reply: AppendReply {
                term: 3,
                success: true,
                conflict_entry_term_first_index: 0,
                conflict_entry_term: 0,
                logs_len: 0,
            },
            peer_id: 0,
            match_index: 2,
            prev_index: 1,
        });
        let s = n.get_state();
        assert_eq!(s.p_state.logs.len(), 3);
        assert_eq!(s.v_state.commit_index, 2);
    }
    #[test]
    fn test_update_term_when_receive_vote_reply() {
        let me = 1;
        let r = create_raft_with_client(vec![], me);
        let node = Node::_new_wiht_timer_and_role(r, false, Role::CANDIDATOR);
        let state = node.get_state();
        let old_term = state.p_state.term;
        let new_term = old_term + 1;
        node.send_event(RaftEvent::VoteReply(RequestVoteReply {
            term: new_term,
            vote_granted: false,
            peer_id: 0,
        }));

        let state = node.get_state();
        assert_eq!(state.v_state.role, Role::FOLLOWER);
        assert_eq!(state.p_state.term, new_term);
    }
    #[test]
    fn test_compare_logs_with_vote() {
        let mut r = create_raft_with_client(vec![], 0);
        let logs = &mut r.persistent_state.logs;
        // same term and index
        logs.truncate(0);
        log_append_entry_with_term(logs, 0);
        log_append_entry_with_term(logs, 1);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 2);

        // same term,same index
        assert!(r.compare_vote_last_entry(3, 2));
        // same term,bigger index
        assert!(!r.compare_vote_last_entry(2, 2));
        // bigger term
        assert!(!r.compare_vote_last_entry(2, 1));
        // smaller term
        assert!(r.compare_vote_last_entry(2, 3));
    }
    #[test]
    fn test_entry_conflict_fast_rollback() {
        {
            let (c1, _) = create_raft_client("test");
            let mut r = create_raft_with_client(vec![c1], 0);
            let term = 3;
            r.persistent_state.term = term;
            let logs = &mut r.persistent_state.logs;
            logs.truncate(0);
            log_append_entry_with_term(logs, 0);
            log_append_entry_with_term(logs, 1);
            log_append_entry_with_term(logs, 2);
            log_append_entry_with_term(logs, 3);
            //  term not exits in reply ,next_index =logs.len
            let n = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);
            let logs_len = 2;
            n.send_event(RaftEvent::AppendReply {
                reply: AppendReply {
                    term,
                    success: false,
                    conflict_entry_term_first_index: 0,
                    conflict_entry_term: 0,
                    logs_len,
                },
                peer_id: 0,
                match_index: 0,
                prev_index: 3,
            });
            let s = n.get_state();

            assert_eq!(*s.v_state.next_index.get(&0).unwrap(), logs_len);
        }

        {
            // term not found in leader, next_index=reply.first_index_in_term
            let (c1, _) = create_raft_client("test");
            let mut r = create_raft_with_client(vec![c1], 0);
            let term = 3;
            r.persistent_state.term = term;
            let logs = &mut r.persistent_state.logs;
            logs.truncate(0);
            log_append_entry_with_term(logs, 0);
            log_append_entry_with_term(logs, 3);
            log_append_entry_with_term(logs, 3);
            log_append_entry_with_term(logs, 3);
            let n = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);
            let logs_len = 2;
            let first_index_in_term_2 = 2;
            // follower log is [0,1,2,2]
            n.send_event(RaftEvent::AppendReply {
                reply: AppendReply {
                    term,
                    success: false,
                    conflict_entry_term_first_index: first_index_in_term_2,
                    conflict_entry_term: 2,
                    logs_len,
                },
                peer_id: 0,
                match_index: 0,
                prev_index: 3,
            });
            let s = n.get_state();

            assert_eq!(
                *s.v_state.next_index.get(&0).unwrap(),
                first_index_in_term_2
            );
        }
        //  term found in leader, not same as prev index term,next_index=last index of term
        {
            let (c1, _) = create_raft_client("test");
            let mut r = create_raft_with_client(vec![c1], 0);
            let term = 3;
            r.persistent_state.term = term;
            let logs = &mut r.persistent_state.logs;
            logs.truncate(0);
            // logs:[0,2,2,3]
            log_append_entry_with_term(logs, 0);
            log_append_entry_with_term(logs, 2);
            log_append_entry_with_term(logs, 3);
            log_append_entry_with_term(logs, 3);
            let n = Node::_new_wiht_timer_and_role(r, false, Role::LEADER);
            let logs_len = 2;
            let first_index = 1;
            // follower log iss [0,2,2,2]
            n.send_event(RaftEvent::AppendReply {
                reply: AppendReply {
                    term,
                    success: false,
                    conflict_entry_term_first_index: first_index,
                    conflict_entry_term: 2,
                    logs_len,
                },
                peer_id: 0,
                match_index: 0,
                prev_index: 3,
            });
            let s = n.get_state();

            // leader log first index in term 2 is 1
            assert_eq!(*s.v_state.next_index.get(&0).unwrap(), 1);
        }
    }
    #[test]
    fn test_follower_append_log() {
        let mut r = create_raft_with_client(vec![], 0);
        r.persistent_state.term = 2;

        // prev entry not exits
        let logs = &mut r.persistent_state.logs;
        // log term is [0,1,2]
        logs.truncate(0);
        log_append_entry_with_term(logs, 0);
        log_append_entry_with_term(logs, 1);
        log_append_entry_with_term(logs, 2);

        let n = Node::_new_disable_timer(r);
        let (tx, rx) = oneshot::channel();
        let mut args = AppendArgs {
            leader_id: 1,
            term: 2,
            prev_log_index: 3,
            prev_log_term: 4,
            leader_commit: 0,
            entrys: vec![],
        };
        n.send_event(RaftEvent::Append(args.clone(), tx));
        let rpc = block_on(rx).unwrap();
        assert!(!rpc.success);

        // prev entry term not match

        let (tx, rx) = oneshot::channel();
        args.prev_log_index = 2;
        args.prev_log_term = 3;
        n.send_event(RaftEvent::Append(args.clone(), tx));
        let rpc = block_on(rx).unwrap();
        assert!(!rpc.success);

        // prev entry match,prev entry is last entry
        // logs: [0,1,2] append prev entry index is 2,term is 2

        let (tx, rx) = oneshot::channel();
        args.prev_log_index = 2;
        args.prev_log_term = 2;
        n.send_event(RaftEvent::Append(args.clone(), tx));
        let rpc: AppendReply = block_on(rx).unwrap();
        assert!(rpc.success);

        // prev entry match,prev entry is second entry,no data in append,logs won't change
        // logs: [0,1,2] append prev entry index is 1,term is 1

        let (tx, rx) = oneshot::channel();
        args.prev_log_index = 1;
        args.prev_log_term = 1;
        n.send_event(RaftEvent::Append(args, tx));
        let rpc: AppendReply = block_on(rx).unwrap();
        assert!(rpc.success);
        let s = n.get_state();
        assert_eq!(s.p_state.logs.len(), 3);

        // prev entry match, some entry after prev entry not match entry in append, discard all conflict entry
        // logs: [0,1,2,2,3] append match in index 2,append data is [2,4,5] after appned should be [0,1,2,2,4,5]

        let mut r = create_raft_with_client(vec![], 0);
        r.persistent_state.term = 3;

        let logs = &mut r.persistent_state.logs;
        logs.truncate(0);
        log_append_entry_with_term(logs, 0);
        log_append_entry_with_term(logs, 1);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 3);
        let n = Node::_new_disable_timer(r);

        let (tx, rx) = oneshot::channel();
        let datas = vec![
            build_entry_with_term(2),
            build_entry_with_term(4),
            build_entry_with_term(5),
        ];

        let args = AppendArgs {
            leader_id: 1,
            term: 5,
            prev_log_index: 2,
            prev_log_term: 2,
            leader_commit: 0,
            entrys: datas,
        };
        n.send_event(RaftEvent::Append(args, tx));
        let rpc: AppendReply = block_on(rx).unwrap();
        assert!(rpc.success);
        let s = n.get_state();
        assert_eq!(s.p_state.logs.len(), 6);
        assert_eq!(s.p_state.logs.get(0).unwrap().term, 0);
        assert_eq!(s.p_state.logs.get(1).unwrap().term, 1);
        assert_eq!(s.p_state.logs.get(2).unwrap().term, 2);
        assert_eq!(s.p_state.logs.get(3).unwrap().term, 2);
        assert_eq!(s.p_state.logs.get(4).unwrap().term, 4);
        assert_eq!(s.p_state.logs.get(5).unwrap().term, 5);

        // prev entry match, entry after prev entry match entry in append,nothing hanpped
        // logs: [0,1,2,2,2,3] append match in index 2,append data is [2,2]

        let mut r = create_raft_with_client(vec![], 0);
        r.persistent_state.term = 3;

        let logs = &mut r.persistent_state.logs;
        logs.truncate(0);
        log_append_entry_with_term(logs, 0);
        log_append_entry_with_term(logs, 1);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 2);
        log_append_entry_with_term(logs, 3);
        let n = Node::_new_disable_timer(r);

        let (tx, rx) = oneshot::channel();
        let datas = vec![build_entry_with_term(2), build_entry_with_term(2)];

        let args = AppendArgs {
            leader_id: 1,
            term: 5,
            prev_log_index: 2,
            prev_log_term: 2,
            leader_commit: 0,
            entrys: datas,
        };
        n.send_event(RaftEvent::Append(args, tx));
        let rpc: AppendReply = block_on(rx).unwrap();
        assert!(rpc.success);
        let s = n.get_state();
        assert_eq!(s.p_state.logs.len(), 6);
        assert_eq!(s.p_state.logs.get(0).unwrap().term, 0);
        assert_eq!(s.p_state.logs.get(1).unwrap().term, 1);
        assert_eq!(s.p_state.logs.get(2).unwrap().term, 2);
        assert_eq!(s.p_state.logs.get(3).unwrap().term, 2);
        assert_eq!(s.p_state.logs.get(4).unwrap().term, 2);
        assert_eq!(s.p_state.logs.get(5).unwrap().term, 3);
    }

    fn log_append_entry_with_term(logs: &mut Logs, term: u64) {
        logs.push(Entry { data: vec![], term });
    }

    fn build_entry_with_term(term: u64) -> Vec<u8> {
        let mut data = vec![];
        let entry = Entry { data: vec![], term };
        labcodec::encode(&entry, &mut data).unwrap();
        data
    }
    #[test]
    fn debug() {}

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

    #[test]
    fn test_log() {
        let mut logs = Logs::new();
        assert_eq!(logs.len(), 1);
        assert!(logs.get(0).is_some());
        assert!(logs.get(1).is_none());
        logs.push(Entry {
            data: vec![],
            term: 1,
        });
        logs.push(Entry {
            data: vec![],
            term: 2,
        });
        assert_eq!(logs.len(), 3);
        assert!(logs.get(2).is_some());
        assert_eq!(logs.last_entry_index_term(), Some((2, 2)));
        assert!(logs.get(4).is_none());

        logs.truncate(2);
        assert_eq!(logs.len(), 2);
        assert_eq!(logs.get(1).unwrap().term, 1);
        assert!(logs.get(2).is_none());

        logs.push(Entry {
            data: vec![],
            term: 1,
        });
        let s = logs.compact(2).unwrap();
        assert_eq!(s.0, 1);
        assert_eq!(s.1, 1);
        assert_eq!(logs.entry_in_log_len(), 1);
        assert_eq!(logs.len(), 3);
    }
}
