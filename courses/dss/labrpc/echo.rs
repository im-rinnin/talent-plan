#![feature(prelude_import)]
#![allow(clippy::new_without_default)]
#[prelude_import]
use std::prelude::rust_2018::*;
#[macro_use]
extern crate std;
mod client {
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use futures::channel::mpsc::UnboundedSender;
    use futures::channel::oneshot;
    use futures::executor::ThreadPool;
    use futures::future::{self, FutureExt};
    use crate::error::{Error, Result};
    use crate::server::RpcFuture;
    pub struct Rpc {
        pub(crate) client_name: String,
        pub(crate) fq_name: &'static str,
        pub(crate) req: Option<Vec<u8>>,
        pub(crate) resp: Option<oneshot::Sender<Result<Vec<u8>>>>,
        pub(crate) hooks: Arc<Mutex<Option<Arc<dyn RpcHooks>>>>,
    }
    impl Rpc {
        pub(crate) fn take_resp_sender(
            &mut self,
        ) -> Option<oneshot::Sender<Result<Vec<u8>>>> {
            self.resp.take()
        }
    }
    impl fmt::Debug for Rpc {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("Rpc")
                .field("client_name", &self.client_name)
                .field("fq_name", &self.fq_name)
                .finish()
        }
    }
    pub trait RpcHooks: Sync + Send + 'static {
        fn before_dispatch(&self, fq_name: &str, req: &[u8]) -> Result<()>;
        fn after_dispatch(
            &self,
            fq_name: &str,
            resp: Result<Vec<u8>>,
        ) -> Result<Vec<u8>>;
    }
    pub struct Client {
        pub(crate) name: String,
        pub(crate) sender: UnboundedSender<Rpc>,
        pub(crate) hooks: Arc<Mutex<Option<Arc<dyn RpcHooks>>>>,
        pub worker: ThreadPool,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Client {
        #[inline]
        fn clone(&self) -> Client {
            Client {
                name: ::core::clone::Clone::clone(&self.name),
                sender: ::core::clone::Clone::clone(&self.sender),
                hooks: ::core::clone::Clone::clone(&self.hooks),
                worker: ::core::clone::Clone::clone(&self.worker),
            }
        }
    }
    impl Client {
        pub fn call<Req, Rsp>(
            &self,
            fq_name: &'static str,
            req: &Req,
        ) -> RpcFuture<Result<Rsp>>
        where
            Req: labcodec::Message,
            Rsp: labcodec::Message + 'static,
        {
            let mut buf = ::alloc::vec::Vec::new();
            if let Err(e) = labcodec::encode(req, &mut buf) {
                return Box::pin(future::err(Error::Encode(e)));
            }
            let (tx, rx) = oneshot::channel();
            let rpc = Rpc {
                client_name: self.name.clone(),
                fq_name,
                req: Some(buf),
                resp: Some(tx),
                hooks: self.hooks.clone(),
            };
            if self.sender.unbounded_send(rpc).is_err() {
                return Box::pin(future::err(Error::Stopped));
            }
            Box::pin(
                rx
                    .then(|res| async move {
                        match res {
                            Ok(Ok(resp)) => {
                                labcodec::decode(&resp).map_err(Error::Decode)
                            }
                            Ok(Err(e)) => Err(e),
                            Err(e) => Err(Error::Recv(e)),
                        }
                    }),
            )
        }
        pub fn set_hooks(&self, hooks: Arc<dyn RpcHooks>) {
            *self.hooks.lock().unwrap() = Some(hooks);
        }
        pub fn clear_hooks(&self) {
            *self.hooks.lock().unwrap() = None;
        }
    }
}
mod error {
    use std::{error, fmt, result};
    use futures::channel::oneshot::Canceled;
    use labcodec::{DecodeError, EncodeError};
    pub enum Error {
        Unimplemented(String),
        Encode(EncodeError),
        Decode(DecodeError),
        Recv(Canceled),
        Timeout,
        Stopped,
        Other(String),
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Error {
        #[inline]
        fn clone(&self) -> Error {
            match self {
                Error::Unimplemented(__self_0) => {
                    Error::Unimplemented(::core::clone::Clone::clone(__self_0))
                }
                Error::Encode(__self_0) => {
                    Error::Encode(::core::clone::Clone::clone(__self_0))
                }
                Error::Decode(__self_0) => {
                    Error::Decode(::core::clone::Clone::clone(__self_0))
                }
                Error::Recv(__self_0) => {
                    Error::Recv(::core::clone::Clone::clone(__self_0))
                }
                Error::Timeout => Error::Timeout,
                Error::Stopped => Error::Stopped,
                Error::Other(__self_0) => {
                    Error::Other(::core::clone::Clone::clone(__self_0))
                }
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Error {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match self {
                Error::Unimplemented(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "Unimplemented",
                        &__self_0,
                    )
                }
                Error::Encode(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "Encode",
                        &__self_0,
                    )
                }
                Error::Decode(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "Decode",
                        &__self_0,
                    )
                }
                Error::Recv(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "Recv",
                        &__self_0,
                    )
                }
                Error::Timeout => ::core::fmt::Formatter::write_str(f, "Timeout"),
                Error::Stopped => ::core::fmt::Formatter::write_str(f, "Stopped"),
                Error::Other(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "Other",
                        &__self_0,
                    )
                }
            }
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for Error {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for Error {
        #[inline]
        fn eq(&self, other: &Error) -> bool {
            let __self_tag = ::core::intrinsics::discriminant_value(self);
            let __arg1_tag = ::core::intrinsics::discriminant_value(other);
            __self_tag == __arg1_tag
                && match (self, other) {
                    (Error::Unimplemented(__self_0), Error::Unimplemented(__arg1_0)) => {
                        *__self_0 == *__arg1_0
                    }
                    (Error::Encode(__self_0), Error::Encode(__arg1_0)) => {
                        *__self_0 == *__arg1_0
                    }
                    (Error::Decode(__self_0), Error::Decode(__arg1_0)) => {
                        *__self_0 == *__arg1_0
                    }
                    (Error::Recv(__self_0), Error::Recv(__arg1_0)) => {
                        *__self_0 == *__arg1_0
                    }
                    (Error::Other(__self_0), Error::Other(__arg1_0)) => {
                        *__self_0 == *__arg1_0
                    }
                    _ => true,
                }
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralEq for Error {}
    #[automatically_derived]
    impl ::core::cmp::Eq for Error {
        #[inline]
        #[doc(hidden)]
        #[no_coverage]
        fn assert_receiver_is_total_eq(&self) -> () {
            let _: ::core::cmp::AssertParamIsEq<String>;
            let _: ::core::cmp::AssertParamIsEq<EncodeError>;
            let _: ::core::cmp::AssertParamIsEq<DecodeError>;
            let _: ::core::cmp::AssertParamIsEq<Canceled>;
        }
    }
    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_fmt(format_args!("{0:?}", self))
        }
    }
    impl error::Error for Error {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            match *self {
                Error::Encode(ref e) => Some(e),
                Error::Decode(ref e) => Some(e),
                Error::Recv(ref e) => Some(e),
                _ => None,
            }
        }
    }
    pub type Result<T> = result::Result<T, Error>;
}
#[macro_use]
mod macros {}
mod network {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
    use futures::executor::ThreadPool;
    use futures::future::FutureExt;
    use futures::select;
    use futures::stream::StreamExt;
    use futures_timer::Delay;
    use log::{debug, error};
    use rand::{thread_rng, Rng};
    use crate::client::{Client, Rpc};
    use crate::error::{Error, Result};
    use crate::server::Server;
    struct EndInfo {
        enabled: bool,
        reliable: bool,
        long_reordering: bool,
        server: Option<Server>,
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for EndInfo {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field4_finish(
                f,
                "EndInfo",
                "enabled",
                &self.enabled,
                "reliable",
                &self.reliable,
                "long_reordering",
                &self.long_reordering,
                "server",
                &&self.server,
            )
        }
    }
    struct Endpoints fn main() {{
        enabled: HashMap<String, bool>,
        servers: HashMap<String, Option<Server>>,
        connections: HashMap<String, Option<String>>,
    }
    struct NetworkCore {
        reliable: AtomicBool,
        long_delays: AtomicBool,
        long_reordering: AtomicBool,
        endpoints: Mutex<Endpoints>,
        count: AtomicUsize,
        sender: UnboundedSender<Rpc>,
        poller: ThreadPool,
        worker: ThreadPool,
    }
    pub struct Network {
        core: Arc<NetworkCore>,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Network {
        #[inline]
        fn clone(&self) -> Network {
            Network {
                core: ::core::clone::Clone::clone(&self.core),
            }
        }
    }
    impl Network {
        pub fn new() -> Network {
            let (net, incoming) = Network::create();
            net.start(incoming);
            net
        }
        pub fn create() -> (Network, UnboundedReceiver<Rpc>) {
            let (sender, incoming) = unbounded();
            let net = Network {
                core: Arc::new(NetworkCore {
                    reliable: AtomicBool::new(true),
                    long_delays: AtomicBool::new(false),
                    long_reordering: AtomicBool::new(false),
                    endpoints: Mutex::new(Endpoints {
                        enabled: HashMap::new(),
                        servers: HashMap::new(),
                        connections: HashMap::new(),
                    }),
                    count: AtomicUsize::new(0),
                    poller: ThreadPool::builder().pool_size(2).create().unwrap(),
                    worker: ThreadPool::new().unwrap(),
                    sender,
                }),
            };
            (net, incoming)
        }
        fn start(&self, mut incoming: UnboundedReceiver<Rpc>) {
            let network = self.clone();
            self.core
                .poller
                .spawn_ok(async move {
                    while let Some(mut rpc) = incoming.next().await {
                        let resp = rpc.take_resp_sender().unwrap();
                        let net = network.clone();
                        network
                            .core
                            .poller
                            .spawn_ok(async move {
                                let res = net.process_rpc(rpc).await;
                                if let Err(e) = resp.send(res) {
                                    {
                                        let lvl = ::log::Level::Error;
                                        if lvl <= ::log::STATIC_MAX_LEVEL
                                            && lvl <= ::log::max_level()
                                        {
                                            ::log::__private_api_log(
                                                format_args!("fail to send resp: {0:?}", e),
                                                lvl,
                                                &(
                                                    "labrpc::network",
                                                    "labrpc::network",
                                                    "labrpc/src/network.rs",
                                                    93u32,
                                                ),
                                            );
                                        }
                                    };
                                }
                            })
                    }
                });
        }
        pub fn add_server(&self, server: Server) {
            let mut eps = self.core.endpoints.lock().unwrap();
            eps.servers.insert(server.core.name.clone(), Some(server));
        }
        pub fn delete_server(&self, name: &str) {
            let mut eps = self.core.endpoints.lock().unwrap();
            if let Some(s) = eps.servers.get_mut(name) {
                *s = None;
            }
        }
        pub fn create_client(&self, name: String) -> Client {
            let sender = self.core.sender.clone();
            let mut eps = self.core.endpoints.lock().unwrap();
            eps.enabled.insert(name.clone(), false);
            eps.connections.insert(name.clone(), None);
            Client {
                name,
                sender,
                worker: self.core.worker.clone(),
                hooks: Arc::new(Mutex::new(None)),
            }
        }
        /// Connects a Client to a server.
        /// a Client can only be connected once in its lifetime.
        pub fn connect(&self, client_name: &str, server_name: &str) {
            let mut eps = self.core.endpoints.lock().unwrap();
            eps.connections.insert(client_name.to_owned(), Some(server_name.to_owned()));
        }
        /// Enable/disable a Client.
        pub fn enable(&self, client_name: &str, enabled: bool) {
            {
                let lvl = ::log::Level::Debug;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                    ::log::__private_api_log(
                        format_args!(
                            "client {0} is {1}", client_name, if enabled { "enabled" }
                            else { "disabled" }
                        ),
                        lvl,
                        &(
                            "labrpc::network",
                            "labrpc::network",
                            "labrpc/src/network.rs",
                            135u32,
                        ),
                    );
                }
            };
            let mut eps = self.core.endpoints.lock().unwrap();
            eps.enabled.insert(client_name.to_owned(), enabled);
        }
        pub fn set_reliable(&self, yes: bool) {
            self.core.reliable.store(yes, Ordering::Release);
        }
        pub fn set_long_reordering(&self, yes: bool) {
            self.core.long_reordering.store(yes, Ordering::Release);
        }
        pub fn set_long_delays(&self, yes: bool) {
            self.core.long_delays.store(yes, Ordering::Release);
        }
        pub fn count(&self, server_name: &str) -> usize {
            let eps = self.core.endpoints.lock().unwrap();
            eps.servers[server_name].as_ref().unwrap().count()
        }
        pub fn total_count(&self) -> usize {
            self.core.count.load(Ordering::Relaxed)
        }
        fn end_info(&self, client_name: &str) -> EndInfo {
            let eps = self.core.endpoints.lock().unwrap();
            let mut server = None;
            if let Some(Some(server_name)) = eps.connections.get(client_name) {
                server = eps.servers[server_name].clone();
            }
            EndInfo {
                enabled: eps.enabled[client_name],
                reliable: self.core.reliable.load(Ordering::Acquire),
                long_reordering: self.core.long_reordering.load(Ordering::Acquire),
                server,
            }
        }
        fn is_server_dead(
            &self,
            client_name: &str,
            server_name: &str,
            server_id: usize,
        ) -> bool {
            let eps = self.core.endpoints.lock().unwrap();
            !eps.enabled[client_name]
                || eps
                    .servers
                    .get(server_name)
                    .map_or(
                        true,
                        |o| {
                            o.as_ref().map(|s| s.core.id != server_id).unwrap_or(true)
                        },
                    )
        }
        async fn process_rpc(&self, rpc: Rpc) -> Result<Vec<u8>> {
            self.core.count.fetch_add(1, Ordering::Relaxed);
            let network = self.clone();
            let end_info = self.end_info(&rpc.client_name);
            {
                let lvl = ::log::Level::Debug;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                    ::log::__private_api_log(
                        format_args!("{0:?} process with {1:?}", rpc, end_info),
                        lvl,
                        &(
                            "labrpc::network",
                            "labrpc::network",
                            "labrpc/src/network.rs",
                            191u32,
                        ),
                    );
                }
            };
            let EndInfo { enabled, reliable, long_reordering, server } = end_info;
            match (enabled, server) {
                (true, Some(server)) => {
                    let short_delay = if !reliable {
                        let ms = thread_rng().gen::<u64>() % 27;
                        Some(ms)
                    } else {
                        None
                    };
                    if !reliable && (thread_rng().gen::<u64>() % 1000) < 100 {
                        Delay::new(Duration::from_secs(short_delay.unwrap())).await;
                        return Err(Error::Timeout);
                    }
                    let drop_reply = !reliable && thread_rng().gen::<u64>() % 1000 < 100;
                    let long_reordering = if long_reordering
                        && thread_rng().gen_range(0, 900) < 600i32
                    {
                        let upper_bound: u64 = 1 + thread_rng().gen_range(0, 2000);
                        Some(200 + thread_rng().gen_range(0, upper_bound))
                    } else {
                        None
                    };
                    process_rpc(
                            short_delay,
                            drop_reply,
                            long_reordering,
                            rpc,
                            network,
                            server,
                        )
                        .await
                }
                _ => {
                    let ms = if self.core.long_delays.load(Ordering::Acquire) {
                        thread_rng().gen::<u64>() % 7000
                    } else {
                        thread_rng().gen::<u64>() % 100
                    };
                    {
                        let lvl = ::log::Level::Debug;
                        if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                            ::log::__private_api_log(
                                format_args!("{0:?} delay {1}ms then timeout", rpc, ms),
                                lvl,
                                &(
                                    "labrpc::network",
                                    "labrpc::network",
                                    "labrpc/src/network.rs",
                                    248u32,
                                ),
                            );
                        }
                    };
                    Delay::new(Duration::from_millis(ms)).await;
                    Err(Error::Timeout)
                }
            }
        }
        /// Spawns a future to run on this net framework.
        pub fn spawn<F>(&self, f: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.core.worker.spawn_ok(f);
        }
        /// Spawns a future to run on this net framework.
        pub fn spawn_poller<F>(&self, f: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.core.poller.spawn_ok(f);
        }
    }
    async fn process_rpc(
        mut delay: Option<u64>,
        drop_reply: bool,
        long_reordering: Option<u64>,
        mut rpc: Rpc,
        network: Network,
        server: Server,
    ) -> Result<Vec<u8>> {
        if let Some(delay) = delay {
            Delay::new(Duration::from_millis(delay)).await;
        }
        delay.take();
        let fq_name = rpc.fq_name;
        let req = rpc.req.take().unwrap();
        if let Some(hooks) = rpc.hooks.lock().unwrap().as_ref() {
            hooks.before_dispatch(fq_name, &req)?;
        }
        let resp = {
            #[allow(dead_code)]
            enum ProcMacroHack {
                Nested = (
                    "futures_crate_path(:: futures) res = server.dispatch(fq_name, & req).fuse() =>\nres, _ =\nserver_dead(Duration :: from_millis(100), network.clone(), & rpc.client_name,\n& server.core.name, server.core.id,).fuse() => Err(Error :: Stopped),",
                    0,
                )
                    .1,
            }
            {
                enum __PrivResult<_0, _1> {
                    _0(_0),
                    _1(_1),
                }
                let __select_result = {
                    let mut _0 = server.dispatch(fq_name, &req).fuse();
                    let mut _1 = server_dead(
                            Duration::from_millis(100),
                            network.clone(),
                            &rpc.client_name,
                            &server.core.name,
                            server.core.id,
                        )
                        .fuse();
                    let mut __poll_fn = |__cx: &mut ::futures::task::Context<'_>| {
                        let mut __any_polled = false;
                        let mut _0 = |__cx: &mut ::futures::task::Context<'_>| {
                            let mut _0 = unsafe {
                                ::core::pin::Pin::new_unchecked(&mut _0)
                            };
                            if ::futures::future::FusedFuture::is_terminated(&_0) {
                                None
                            } else {
                                Some(
                                    ::futures::future::FutureExt::poll_unpin(&mut _0, __cx)
                                        .map(__PrivResult::_0),
                                )
                            }
                        };
                        let _0: &mut dyn FnMut(
                            &mut ::futures::task::Context<'_>,
                        ) -> Option<::futures::task::Poll<_>> = &mut _0;
                        let mut _1 = |__cx: &mut ::futures::task::Context<'_>| {
                            let mut _1 = unsafe {
                                ::core::pin::Pin::new_unchecked(&mut _1)
                            };
                            if ::futures::future::FusedFuture::is_terminated(&_1) {
                                None
                            } else {
                                Some(
                                    ::futures::future::FutureExt::poll_unpin(&mut _1, __cx)
                                        .map(__PrivResult::_1),
                                )
                            }
                        };
                        let _1: &mut dyn FnMut(
                            &mut ::futures::task::Context<'_>,
                        ) -> Option<::futures::task::Poll<_>> = &mut _1;
                        let mut __select_arr = [_0, _1];
                        ::futures::async_await::shuffle(&mut __select_arr);
                        for poller in &mut __select_arr {
                            let poller: &mut &mut dyn FnMut(
                                &mut ::futures::task::Context<'_>,
                            ) -> Option<::futures::task::Poll<_>> = poller;
                            match poller(__cx) {
                                Some(x @ ::futures::task::Poll::Ready(_)) => return x,
                                Some(::futures::task::Poll::Pending) => {
                                    __any_polled = true;
                                }
                                None => {}
                            }
                        }
                        if !__any_polled {
                            {
                                ::std::rt::begin_panic(
                                    "all futures in select! were completed,\
                    but no `complete =>` handler was provided",
                                )
                            }
                        } else {
                            ::futures::task::Poll::Pending
                        }
                    };
                    ::futures::future::poll_fn(__poll_fn).await
                };
                match __select_result {
                    __PrivResult::_0(res) => res,
                    __PrivResult::_1(_) => Err(Error::Stopped),
                }
            }
        };
        let resp = if let Some(hooks) = rpc.hooks.lock().unwrap().as_ref() {
            hooks.after_dispatch(fq_name, resp)?
        } else {
            resp?
        };
        let client_name = &rpc.client_name;
        let server_name = &server.core.name;
        let server_id = server.core.id;
        if network.is_server_dead(client_name, server_name, server_id) {
            return Err(Error::Stopped);
        }
        if drop_reply {
            return Err(Error::Timeout);
        }
        if let Some(reordering) = long_reordering {
            {
                let lvl = ::log::Level::Debug;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                    ::log::__private_api_log(
                        format_args!(
                            "{0:?} next long reordering {1}ms", rpc, reordering
                        ),
                        lvl,
                        &(
                            "labrpc::network",
                            "labrpc::network",
                            "labrpc/src/network.rs",
                            333u32,
                        ),
                    );
                }
            };
            Delay::new(Duration::from_millis(reordering)).await;
            Ok(resp)
        } else {
            Ok(resp)
        }
    }
    /// Checks if the specified server killed.
    ///
    /// It will return when the server is killed.
    async fn server_dead(
        interval: Duration,
        net: Network,
        client_name: &str,
        server_name: &str,
        server_id: usize,
    ) {
        loop {
            Delay::new(interval).await;
            if net.is_server_dead(client_name, server_name, server_id) {
                {
                    let lvl = ::log::Level::Debug;
                    if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                        ::log::__private_api_log(
                            format_args!("{0:?} is dead", server_name),
                            lvl,
                            &(
                                "labrpc::network",
                                "labrpc::network",
                                "labrpc/src/network.rs",
                                354u32,
                            ),
                        );
                    }
                };
                return;
            }
        }
    }
}
mod server {
    use std::collections::hash_map::{Entry, HashMap};
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use futures::future::{self, BoxFuture};
    use crate::error::{Error, Result};
    static ID_ALLOC: AtomicUsize = AtomicUsize::new(0);
    pub type RpcFuture<T> = BoxFuture<'static, T>;
    pub type Handler = dyn FnOnce(&[u8]) -> RpcFuture<Result<Vec<u8>>>;
    pub trait HandlerFactory: Sync + Send + 'static {
        fn handler(&self, name: &'static str) -> Box<Handler>;
    }
    pub struct ServerBuilder {
        name: String,
        pub(crate) services: HashMap<&'static str, Box<dyn HandlerFactory>>,
    }
    impl ServerBuilder {
        pub fn new(name: String) -> ServerBuilder {
            ServerBuilder {
                name,
                services: HashMap::new(),
            }
        }
        pub fn add_service(
            &mut self,
            service_name: &'static str,
            factory: Box<dyn HandlerFactory>,
        ) -> Result<()> {
            match self.services.entry(service_name) {
                Entry::Occupied(_) => {
                    Err(
                        Error::Other({
                            let res = ::alloc::fmt::format(
                                format_args!("{0} has already registered", service_name),
                            );
                            res
                        }),
                    )
                }
                Entry::Vacant(entry) => {
                    entry.insert(factory);
                    Ok(())
                }
            }
        }
        pub fn build(self) -> Server {
            Server {
                core: Arc::new(ServerCore {
                    name: self.name,
                    services: self.services,
                    id: ID_ALLOC.fetch_add(1, Ordering::Relaxed),
                    count: AtomicUsize::new(0),
                }),
            }
        }
    }
    pub(crate) struct ServerCore {
        pub(crate) name: String,
        pub(crate) id: usize,
        pub(crate) services: HashMap<&'static str, Box<dyn HandlerFactory>>,
        pub(crate) count: AtomicUsize,
    }
    pub struct Server {
        pub(crate) core: Arc<ServerCore>,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Server {
        #[inline]
        fn clone(&self) -> Server {
            Server {
                core: ::core::clone::Clone::clone(&self.core),
            }
        }
    }
    impl Server {
        pub fn count(&self) -> usize {
            self.core.count.load(Ordering::Relaxed)
        }
        pub fn name(&self) -> &str {
            &self.core.name
        }
        pub(crate) fn dispatch(
            &self,
            fq_name: &'static str,
            req: &[u8],
        ) -> RpcFuture<Result<Vec<u8>>> {
            self.core.count.fetch_add(1, Ordering::Relaxed);
            let mut names = fq_name.split('.');
            let service_name = match names.next() {
                Some(n) => n,
                None => {
                    return Box::pin(
                        future::err(
                            Error::Unimplemented({
                                let res = ::alloc::fmt::format(
                                    format_args!("unknown {0}", fq_name),
                                );
                                res
                            }),
                        ),
                    );
                }
            };
            let method_name = match names.next() {
                Some(n) => n,
                None => {
                    return Box::pin(
                        future::err(
                            Error::Unimplemented({
                                let res = ::alloc::fmt::format(
                                    format_args!("unknown {0}", fq_name),
                                );
                                res
                            }),
                        ),
                    );
                }
            };
            if let Some(factory) = self.core.services.get(service_name) {
                let handle = factory.handler(method_name);
                handle(req)
            } else {
                Box::pin(
                    future::err(
                        Error::Unimplemented({
                            let res = ::alloc::fmt::format(
                                format_args!("unknown {0}", fq_name),
                            );
                            res
                        }),
                    ),
                )
            }
        }
    }
    impl fmt::Debug for Server {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("Server")
                .field("name", &self.core.name)
                .field("id", &self.core.id)
                .finish()
        }
    }
}
pub use self::client::{Client, Rpc, RpcHooks};
pub use self::error::{Error, Result};
pub use self::network::Network;
pub use self::server::{Handler, HandlerFactory, RpcFuture, Server, ServerBuilder};
