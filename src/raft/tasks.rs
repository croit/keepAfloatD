use futures::FutureExt;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Clone, Copy)]
pub(super) enum CleanExit {
    Allowed,
    Unexpected,
}

pub(super) struct SupervisedTask {
    pub(super) name: &'static str,
    pub(super) handle: JoinHandle<anyhow::Result<()>>,
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

pub(super) fn spawn_supervised_task<F>(
    name: &'static str,
    shutdown: Arc<AtomicBool>,
    failure_tx: mpsc::UnboundedSender<String>,
    clean_exit: CleanExit,
    future: F,
) -> SupervisedTask
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let handle = tokio::spawn(async move {
        let result = match AssertUnwindSafe(future).catch_unwind().await {
            Ok(Ok(()))
                if shutdown.load(Ordering::SeqCst) || matches!(clean_exit, CleanExit::Allowed) =>
            {
                Ok(())
            }
            Ok(Ok(())) => Err(anyhow::anyhow!("{name} exited unexpectedly")),
            Ok(Err(error)) => Err(error.context(name)),
            Err(payload) => Err(anyhow::anyhow!(
                "{name} panicked: {}",
                panic_message(payload)
            )),
        };
        if let Err(error) = &result {
            let _ = failure_tx.send(format!("{error:#}"));
        }
        result
    });
    SupervisedTask { name, handle }
}

pub(super) async fn stop_supervised_tasks(
    tasks: Vec<SupervisedTask>,
    timeout: Duration,
) -> anyhow::Result<()> {
    for task in &tasks {
        task.handle.abort();
    }

    let mut failures = Vec::new();
    for task in tasks {
        match tokio::time::timeout(timeout, task.handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => failures.push(format!("{}: {error:#}", task.name)),
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(error)) => failures.push(format!("{} join: {error}", task.name)),
            Err(_) => failures.push(format!("{} did not stop", task.name)),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(failures.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::{CleanExit, SupervisedTask, spawn_supervised_task, stop_supervised_tasks};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::mpsc;

    async fn supervised_failure<F>(future: F) -> (String, String)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        let task = spawn_supervised_task(
            "test task",
            Arc::new(AtomicBool::new(false)),
            failure_tx,
            CleanExit::Unexpected,
            future,
        );
        let result = format!("{:#}", task.handle.await.unwrap().unwrap_err());
        (result, failure_rx.recv().await.unwrap())
    }

    #[tokio::test]
    async fn supervisor_preserves_errors_and_every_panic_payload_shape() {
        for (result, reported) in [
            supervised_failure(async { Err(anyhow::anyhow!("future error")) }).await,
            supervised_failure(async {
                panic!("borrowed panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await,
            supervised_failure(async {
                std::panic::panic_any(String::from("owned panic"));
                #[allow(unreachable_code)]
                Ok(())
            })
            .await,
            supervised_failure(async {
                std::panic::panic_any(7_u8);
                #[allow(unreachable_code)]
                Ok(())
            })
            .await,
        ] {
            assert!(result.contains("test task"));
            assert_eq!(reported, result);
        }
    }

    #[tokio::test]
    async fn shutdown_allows_an_unexpected_policy_task_to_finish_cleanly() {
        let shutdown = Arc::new(AtomicBool::new(true));
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        let task = spawn_supervised_task(
            "stopping task",
            shutdown,
            failure_tx,
            CleanExit::Unexpected,
            async { Ok(()) },
        );

        assert!(task.handle.await.unwrap().is_ok());
        assert!(failure_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_shutdown_reports_join_panic_and_outer_timeout() {
        let panicked = tokio::spawn(async {
            panic!("unwrapped task panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !panicked.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = stop_supervised_tasks(
            vec![SupervisedTask {
                name: "panicked task",
                handle: panicked,
            }],
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("panicked task join"));

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let blocked = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(50));
            Ok(())
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let error = stop_supervised_tasks(
            vec![SupervisedTask {
                name: "blocked task",
                handle: blocked,
            }],
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("blocked task did not stop"));
    }
}
