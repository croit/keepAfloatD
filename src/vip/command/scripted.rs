use super::CommandRunner;
use futures::future::BoxFuture;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::io;
use std::process::{ExitStatus, Output};
use std::sync::Mutex;
use std::time::Duration;
use tokio::process::Command;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Invocation {
    pub(crate) program: OsString,
    pub(crate) arguments: Vec<OsString>,
    pub(crate) timeout: Duration,
    pub(crate) capture: bool,
}

impl Invocation {
    fn new(command: &Command, timeout: Duration, capture: bool) -> Self {
        Self {
            program: command.as_std().get_program().to_owned(),
            arguments: command.as_std().get_args().map(OsString::from).collect(),
            timeout,
            capture,
        }
    }
}

type Reply = BoxFuture<'static, io::Result<Output>>;

#[derive(Default)]
pub(crate) struct ScriptedRunner {
    replies: Mutex<HashMap<Invocation, VecDeque<Reply>>>,
    calls: Mutex<Vec<Invocation>>,
}

impl ScriptedRunner {
    pub(crate) fn expect_output(
        &self,
        command: &Command,
        timeout: Duration,
        result: io::Result<Output>,
    ) {
        self.expect(command, timeout, true, Box::pin(async { result }));
    }

    pub(crate) fn expect_status(
        &self,
        command: &Command,
        timeout: Duration,
        result: io::Result<ExitStatus>,
    ) {
        self.expect_status_future(command, timeout, async { result });
    }

    pub(crate) fn expect_status_future(
        &self,
        command: &Command,
        timeout: Duration,
        result: impl Future<Output = io::Result<ExitStatus>> + Send + 'static,
    ) {
        self.expect(
            command,
            timeout,
            false,
            Box::pin(async {
                result.await.map(|status| Output {
                    status,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }),
        );
    }

    pub(crate) fn expect_output_future(
        &self,
        command: &Command,
        timeout: Duration,
        result: impl Future<Output = io::Result<Output>> + Send + 'static,
    ) {
        self.expect(command, timeout, true, Box::pin(result));
    }

    fn expect(&self, command: &Command, timeout: Duration, capture: bool, reply: Reply) {
        self.replies
            .lock()
            .unwrap()
            .entry(Invocation::new(command, timeout, capture))
            .or_default()
            .push_back(reply);
    }

    pub(crate) fn remaining(&self, command: &Command, timeout: Duration, capture: bool) -> usize {
        self.replies
            .lock()
            .unwrap()
            .get(&Invocation::new(command, timeout, capture))
            .map_or(0, VecDeque::len)
    }

    pub(crate) fn clear(&self, command: &Command, timeout: Duration, capture: bool) {
        self.replies
            .lock()
            .unwrap()
            .remove(&Invocation::new(command, timeout, capture));
    }

    pub(crate) fn pause_next(
        &self,
        command: &Command,
        timeout: Duration,
        capture: bool,
    ) -> tokio::sync::oneshot::Sender<()> {
        let call = Invocation::new(command, timeout, capture);
        let mut replies = self.replies.lock().unwrap();
        let queue = replies
            .get_mut(&call)
            .expect("command must be scripted before pausing");
        let reply = queue.pop_front().expect("command needs a pending reply");
        let (release, wait) = tokio::sync::oneshot::channel();
        queue.push_front(Box::pin(async move {
            let _ = wait.await;
            reply.await
        }));
        release
    }

    pub(crate) fn calls(&self) -> Vec<Invocation> {
        self.calls.lock().unwrap().clone()
    }

    pub(crate) fn remaining_where(&self, predicate: impl Fn(&Invocation) -> bool) -> usize {
        self.replies
            .lock()
            .unwrap()
            .iter()
            .filter(|(call, _)| predicate(call))
            .map(|(_, replies)| replies.len())
            .sum()
    }

    pub(crate) fn clear_where(&self, predicate: impl Fn(&Invocation) -> bool) {
        self.replies
            .lock()
            .unwrap()
            .retain(|call, _| !predicate(call));
    }

    pub(crate) fn assert_finished(&self) {
        let replies = self.replies.lock().unwrap();
        let pending: Vec<_> = replies
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(call, queue)| (call, queue.len()))
            .collect();
        assert!(pending.is_empty(), "unconsumed VIP commands: {pending:?}");
    }

    fn take(&self, command: &Command, timeout: Duration, capture: bool) -> Reply {
        let call = Invocation::new(command, timeout, capture);
        self.calls.lock().unwrap().push(call.clone());
        let result = self
            .replies
            .lock()
            .unwrap()
            .get_mut(&call)
            .and_then(VecDeque::pop_front);
        result.unwrap_or_else(|| panic!("unexpected VIP command: {call:?}"))
    }
}

impl CommandRunner for ScriptedRunner {
    fn status<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<ExitStatus>> {
        Box::pin(async move {
            self.take(command, timeout, false)
                .await
                .map(|output| output.status)
        })
    }

    fn output<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<Output>> {
        Box::pin(async move { self.take(command, timeout, true).await })
    }
}
