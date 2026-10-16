use crate::error::map_error;
use sqlx_core::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

type Reply = flume::Sender<Result<(), Error>>;
enum Command {
    Execute(
        String,
        crate::OdbcArguments,
        bool,
        flume::Sender<crate::execution::Output>,
    ),
    Describe(
        String,
        flume::Sender<Result<sqlx_core::describe::Describe<crate::Odbc>, Error>>,
    ),
    ClearCache(Reply),
    Begin(Reply),
    Commit(Reply),
    Ping(Reply),
    Flush(Reply),
    Rollback,
    Close(Reply),
}

pub(crate) trait NativeConnection: 'static {
    fn cache_size(&self) -> usize {
        0
    }
    fn clear_cache(&mut self) {}
    fn set_persistent(&mut self, _persistent: bool) {}
    fn describe(
        &mut self,
        _sql: &str,
    ) -> Result<sqlx_core::describe::Describe<crate::Odbc>, Error> {
        Err(crate::unavailable())
    }
    fn execute(
        &mut self,
        _sql: &str,
        _arguments: crate::OdbcArguments,
        _output: &flume::Sender<crate::execution::Output>,
    ) -> Result<(), Error> {
        Err(crate::unavailable())
    }
    fn begin(&mut self) -> Result<(), Error> {
        Err(crate::unavailable())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Err(crate::unavailable())
    }
    fn ping(&mut self) -> Result<(), Error>;
    fn rollback(&mut self) -> Result<(), Error>;
}
#[derive(Debug)]
pub(crate) struct Worker {
    commands: Option<flume::Sender<Command>>,
    stopped: flume::Receiver<()>,
    retired: Arc<AtomicBool>,
    rollback_pending: Arc<AtomicBool>,
    cache_size: Arc<std::sync::atomic::AtomicUsize>,
}
impl Worker {
    pub(crate) async fn connect(
        connection_string: String,
        login_timeout_sec: Option<u32>,
        capacity: usize,
        buffered: bool,
    ) -> Result<Self, Error> {
        Self::spawn(move || {
            odbc_api::environment()
                .map_err(map_error)?
                .connect_with_connection_string(
                    &connection_string,
                    odbc_api::ConnectionOptions {
                        login_timeout_sec,
                        ..Default::default()
                    },
                )
                .map_err(map_error)
                .map(|connection| {
                    crate::cache::CachedConnection::new(connection, capacity, buffered)
                })
        })
        .await
    }

    pub(crate) async fn spawn<N, F>(connect: F) -> Result<Self, Error>
    where
        N: NativeConnection,
        F: FnOnce() -> Result<N, Error> + Send + 'static,
    {
        let (commands, receiver) = flume::bounded(16);
        let (ready, connected) = flume::bounded(1);
        let (running, stopped_signal) = flume::bounded::<()>(0);
        let retired = Arc::new(AtomicBool::new(false));
        let rollback_pending = Arc::new(AtomicBool::new(false));
        let cache_size = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker_cache_size = cache_size.clone();
        let worker_retired = retired.clone();
        let worker_pending = rollback_pending.clone();
        std::thread::Builder::new()
            .name("sqlx-odbc".into())
            .spawn(move || {
                let _running = running;
                let mut native = match connect() {
                    Ok(native) => native,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                while let Ok(command) = receiver.recv() {
                    if worker_retired.load(Ordering::Acquire) {
                        break;
                    }
                    match command {
                        Command::Execute(sql, arguments, persistent, output) => {
                            native.set_persistent(persistent);
                            if let Err(error) = native.execute(&sql, arguments, &output) {
                                let _ = output.send(Err(error));
                                break;
                            }
                        }
                        Command::ClearCache(reply) => {
                            native.clear_cache();
                            worker_cache_size.store(0, Ordering::Release);
                            let _ = reply.send(Ok(()));
                        }
                        Command::Describe(sql, reply) => {
                            let _ = reply.send(native.describe(&sql));
                        }
                        Command::Begin(reply) => {
                            let result = native.begin();
                            let failed = result.is_err();
                            if reply.send(result).is_err() || failed {
                                break;
                            }
                        }
                        Command::Commit(reply) => {
                            let result = native.commit();
                            let failed = result.is_err();
                            if reply.send(result).is_err() || failed {
                                break;
                            }
                        }
                        Command::Ping(reply) => {
                            let result = native.ping();
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed {
                                break;
                            }
                        }
                        Command::Flush(reply) => {
                            worker_cache_size.store(native.cache_size(), Ordering::Release);
                            let _ = reply.send(Ok(()));
                        }
                        Command::Rollback => {
                            if native.rollback().is_err() {
                                break;
                            }
                            worker_pending.store(false, Ordering::Release);
                        }
                        Command::Close(reply) => {
                            drop(native);
                            let _ = reply.send(Ok(()));
                            return;
                        }
                    }
                }
                worker_retired.store(true, Ordering::Release);
            })
            .map_err(Error::Io)?;
        let worker = Self {
            commands: Some(commands),
            stopped: stopped_signal,
            retired,
            rollback_pending,
            cache_size,
        };
        connected.recv_async().await.map_err(|_| stopped())??;
        Ok(worker)
    }
    async fn request(&mut self, make_command: impl FnOnce(Reply) -> Command) -> Result<(), Error> {
        if self.retired.load(Ordering::Acquire) {
            return Err(stopped());
        }
        let (reply, response) = flume::bounded(1);
        self.commands
            .as_ref()
            .ok_or_else(stopped)?
            .send_async(make_command(reply))
            .await
            .map_err(|_| stopped())?;
        let result = match futures_util::future::select(
            Box::pin(response.recv_async()),
            Box::pin(self.stopped.recv_async()),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => result.map_err(|_| stopped())?,
            futures_util::future::Either::Right(_) => {
                response.try_recv().unwrap_or_else(|_| Err(stopped()))
            }
        };
        result
    }
    pub(crate) async fn execute(
        &mut self,
        sql: String,
        arguments: crate::OdbcArguments,
        persistent: bool,
    ) -> Result<flume::Receiver<crate::execution::Output>, Error> {
        if self.retired.load(Ordering::Acquire) {
            return Err(stopped());
        }
        let (output, rows) = flume::bounded(32);
        self.commands
            .as_ref()
            .ok_or_else(stopped)?
            .send_async(Command::Execute(sql, arguments, persistent, output))
            .await
            .map_err(|_| stopped())?;
        Ok(rows)
    }
    pub(crate) async fn receive(
        &self,
        rows: &flume::Receiver<crate::execution::Output>,
    ) -> Result<Option<crate::execution::Output>, Error> {
        match futures_util::future::select(
            Box::pin(rows.recv_async()),
            Box::pin(self.stopped.recv_async()),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => Ok(result.ok()),
            futures_util::future::Either::Right(_) => match rows.try_recv() {
                Ok(row) => Ok(Some(row)),
                Err(_) => Err(stopped()),
            },
        }
    }
    pub(crate) async fn describe(
        &mut self,
        sql: String,
    ) -> Result<sqlx_core::describe::Describe<crate::Odbc>, Error> {
        let (reply, response) = flume::bounded(1);
        self.commands
            .as_ref()
            .ok_or_else(stopped)?
            .send_async(Command::Describe(sql, reply))
            .await
            .map_err(|_| stopped())?;
        let result = match futures_util::future::select(
            Box::pin(response.recv_async()),
            Box::pin(self.stopped.recv_async()),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => result.map_err(|_| stopped())?,
            futures_util::future::Either::Right(_) => {
                response.try_recv().unwrap_or_else(|_| Err(stopped()))
            }
        };
        result
    }
    pub(crate) async fn begin(&mut self) -> Result<(), Error> {
        self.request(Command::Begin).await
    }
    pub(crate) async fn commit(&mut self) -> Result<(), Error> {
        self.request(Command::Commit).await
    }
    pub(crate) fn cached_statements_size(&self) -> usize {
        self.cache_size.load(Ordering::Acquire)
    }
    pub(crate) async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        self.request(Command::ClearCache).await
    }
    pub(crate) async fn ping(&mut self) -> Result<(), Error> {
        self.request(Command::Ping).await
    }
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        self.request(Command::Flush).await
    }
    pub(crate) fn should_flush(&self) -> bool {
        self.rollback_pending.load(Ordering::Acquire)
    }
    pub(crate) fn start_rollback(&mut self) {
        if self.rollback_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        if self
            .commands
            .as_ref()
            .is_none_or(|commands| commands.try_send(Command::Rollback).is_err())
        {
            self.retired.store(true, Ordering::Release);
            self.commands.take();
        }
    }
    pub(crate) async fn close(mut self) -> Result<(), Error> {
        self.request(Command::Close).await
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::Release);
        self.commands.take();
    }
}
fn stopped() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "ODBC worker stopped",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_executor::block_on;
    use std::{rc::Rc, time::Duration};

    #[test]
    fn dropping_full_row_stream_unblocks_worker() {
        struct Producer;
        impl NativeConnection for Producer {
            fn ping(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn rollback(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn execute(
                &mut self,
                _: &str,
                _: crate::OdbcArguments,
                output: &flume::Sender<crate::execution::Output>,
            ) -> Result<(), Error> {
                for _ in 0..1000 {
                    if output
                        .send(Ok(sqlx_core::Either::Left(
                            crate::OdbcQueryResult::default(),
                        )))
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(())
            }
        }
        let mut worker = block_on(Worker::spawn(|| Ok(Producer))).unwrap();
        let rows = block_on(worker.execute("test".into(), Default::default(), true)).unwrap();
        assert_eq!(rows.capacity(), Some(32));
        rows.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        drop(rows);
        block_on(worker.ping()).unwrap();
        block_on(worker.close()).unwrap();
    }
    struct Mock {
        events: flume::Sender<(&'static str, std::thread::ThreadId)>,
        fail: bool,
        _not_send: Rc<()>,
    }
    impl Mock {
        fn event(&self, event: &'static str) {
            self.events
                .send((event, std::thread::current().id()))
                .unwrap();
        }
    }
    impl NativeConnection for Mock {
        fn ping(&mut self) -> Result<(), Error> {
            self.event("ping");
            Ok(())
        }
        fn rollback(&mut self) -> Result<(), Error> {
            self.event("rollback");
            if self.fail {
                Err(stopped())
            } else {
                Ok(())
            }
        }
    }
    impl Drop for Mock {
        fn drop(&mut self) {
            self.event("drop");
        }
    }
    fn mock(events: flume::Sender<(&'static str, std::thread::ThreadId)>, fail: bool) -> Mock {
        let native = Mock {
            events,
            fail,
            _not_send: Rc::new(()),
        };
        native.event("connect");
        native
    }
    #[test]
    fn native_lifecycle_and_rollback_are_ordered_on_worker() {
        let (events, observed) = flume::unbounded();
        let mut worker = block_on(Worker::spawn(move || Ok(mock(events, false)))).unwrap();
        worker.start_rollback();
        block_on(worker.ping()).unwrap();
        block_on(worker.flush()).unwrap();
        assert!(!worker.should_flush());
        block_on(worker.close()).unwrap();
        let events: Vec<_> = observed.drain().collect();
        assert_eq!(
            events.iter().map(|event| event.0).collect::<Vec<_>>(),
            ["connect", "rollback", "ping", "drop"]
        );
        assert!(events.iter().all(|event| event.1 == events[0].1));
        assert_ne!(events[0].1, std::thread::current().id());
    }
    #[test]
    fn failed_rollback_retires_connection() {
        let (events, observed) = flume::unbounded();
        let mut worker = block_on(Worker::spawn(move || Ok(mock(events, true)))).unwrap();
        worker.start_rollback();
        assert!(block_on(worker.ping()).is_err());
        assert!(block_on(worker.flush()).is_err());
        drop(worker);
        let events: Vec<_> = observed.iter().map(|event| event.0).collect();
        assert_eq!(events, ["connect", "rollback", "drop"]);
    }
    #[test]
    fn cancelled_connect_drops_native_on_worker() {
        let (events, observed) = flume::unbounded();
        let (release, wait) = flume::bounded(1);
        let mut future = Box::pin(Worker::spawn(move || {
            wait.recv().unwrap();
            Ok(mock(events, false))
        }));
        let waker = futures_util::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(std::future::Future::poll(future.as_mut(), &mut context).is_pending());
        drop(future);
        release.send(()).unwrap();
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap().0,
            "connect"
        );
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap().0,
            "drop"
        );
    }
    #[test]
    fn worker_panic_is_reported_without_hanging() {
        struct Panics;
        impl NativeConnection for Panics {
            fn ping(&mut self) -> Result<(), Error> {
                panic!("simulated worker failure")
            }
            fn rollback(&mut self) -> Result<(), Error> {
                Ok(())
            }
        }
        let mut worker = block_on(Worker::spawn(|| Ok(Panics))).unwrap();
        assert!(block_on(worker.ping()).is_err());
        assert!(block_on(worker.flush()).is_err());
    }
    #[test]
    fn abandoned_requests_and_full_queue_do_not_block_rollback() {
        struct Blocked(flume::Receiver<()>);
        impl NativeConnection for Blocked {
            fn ping(&mut self) -> Result<(), Error> {
                self.0.recv().unwrap();
                Ok(())
            }
            fn rollback(&mut self) -> Result<(), Error> {
                Ok(())
            }
        }
        let (release, wait) = flume::bounded(1);
        let mut worker = block_on(Worker::spawn(|| Ok(Blocked(wait)))).unwrap();
        let waker = futures_util::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        let mut pending = Box::pin(worker.ping());
        assert!(std::future::Future::poll(pending.as_mut(), &mut context).is_pending());
        drop(pending);
        for _ in 0..17 {
            let (reply, _) = flume::bounded(1);
            let _ = worker
                .commands
                .as_ref()
                .unwrap()
                .try_send(Command::Flush(reply));
        }
        worker.start_rollback();
        assert!(worker.retired.load(Ordering::Acquire));
        assert!(block_on(worker.flush()).is_err());
        let _ = release.send(());
    }
    #[test]
    fn dropping_worker_releases_idle_native_connection() {
        let (events, observed) = flume::unbounded();
        let worker = block_on(Worker::spawn(move || Ok(mock(events, false)))).unwrap();
        drop(worker);
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap().0,
            "connect"
        );
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap().0,
            "drop"
        );
    }
}
