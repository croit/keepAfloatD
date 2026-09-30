use super::*;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

#[derive(Debug, PartialEq)]
enum LegacyEvent {
    ProofRejected,
    UnhealthyFallback,
    Acknowledged,
}

struct LegacySubmit {
    follower: AtomicU64,
    fenced_node: AtomicU64,
    acknowledge: Semaphore,
}

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let size = stream.read_u32().await?;
    anyhow::ensure!(size <= 4096, "oversized test submit frame");
    let mut body = vec![0; size as usize];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> anyhow::Result<()> {
    stream.write_u32(body.len() as u32).await?;
    stream.write_all(body).await?;
    Ok(())
}

async fn proxy_submit(
    mut client: TcpStream,
    upstream: u16,
    legacy: &LegacySubmit,
    events: &mpsc::UnboundedSender<LegacyEvent>,
) -> anyhow::Result<()> {
    let body = read_frame(&mut client).await?;
    let envelope: serde_json::Value = serde_json::from_slice(&body)?;
    let request = &envelope["request"];
    let fenced = legacy.fenced_node.load(Ordering::SeqCst);
    if let Some(proof) = request.get("HealthUpdateWithProof") {
        let node = proof["node_id"].as_u64().unwrap();
        legacy.follower.store(node, Ordering::SeqCst);
        if node == fenced {
            anyhow::ensure!(
                proof["healthy"] == true,
                "the local probe must stay healthy"
            );
            events.send(LegacyEvent::ProofRejected)?;
            return Ok(());
        }
    }
    if let Some(health) = request.get("HealthUpdate")
        && health["node_id"].as_u64() == Some(fenced)
    {
        anyhow::ensure!(
            health["healthy"] == false,
            "legacy fallback must be unhealthy"
        );
        events.send(LegacyEvent::UnhealthyFallback)?;
        let _permit = legacy.acknowledge.acquire().await?;
        // Delay the legacy ACK so the test distinguishes early fencing from completion.
        write_frame(&mut client, br#"{"ok":true,"message":""}"#).await?;
        let mut trailing = [0; 1];
        anyhow::ensure!(
            client.read(&mut trailing).await? == 0,
            "unexpected trailing request"
        );
        events.send(LegacyEvent::Acknowledged)?;
        return Ok(());
    }
    let mut server = test_cluster_connect(upstream).await?;
    write_frame(&mut server, &body).await?;
    write_frame(&mut client, &read_frame(&mut server).await?).await
}

async fn serve_proxy(
    listener: TcpListener,
    upstream: u16,
    legacy: Arc<LegacySubmit>,
    events: mpsc::UnboundedSender<LegacyEvent>,
) -> anyhow::Result<()> {
    let mut requests = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, _) = accepted?;
                let legacy = legacy.clone();
                let events = events.clone();
                // JoinSet cancellation stops every in-flight connection with its proxy.
                requests.spawn(async move {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        proxy_submit(client, upstream, &legacy, &events),
                    ).await?
                });
            }
            result = requests.join_next(), if !requests.is_empty() => {
                result.unwrap()??;
            }
        }
    }
}

async fn notifications(log: &Path, vip: IpAddr) -> anyhow::Result<Vec<String>> {
    let prefix = format!("INSTANCE {vip} ");
    let content = match tokio::fs::read_to_string(log).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    Ok(content
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix).map(str::to_owned))
        .collect())
}

async fn wait_notifications(log: &Path, vip: IpAddr, expected: &[String]) -> anyhow::Result<()> {
    let mut poll = tokio::time::interval(Duration::from_millis(10));
    loop {
        poll.tick().await;
        let states = notifications(log, vip).await?;
        anyhow::ensure!(
            expected.starts_with(&states),
            "unexpected notify sequence: {states:?}"
        );
        if states.len() == expected.len() {
            return Ok(());
        }
    }
}

async fn remove_fixtures(directory: &Path) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            match tokio::fs::remove_dir_all(directory).await {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                // A detached notify hook may still open its log after daemon shutdown.
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => (),
                Err(error) => return Err(anyhow::Error::from(error)),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_proof_rejection_releases_with_backup_and_recovers() {
    let _guard = CLUSTER_TEST_LOCK.lock().await;
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind((CLUSTER_TEST_ADDR, 0)).await.unwrap());
    }
    let ports = free_ports(6);
    let legacy = Arc::new(LegacySubmit {
        follower: AtomicU64::new(0),
        fenced_node: AtomicU64::new(0),
        acknowledge: Semaphore::new(0),
    });
    let (events, mut received) = mpsc::unbounded_channel();
    let mut proxies = Vec::new();
    let mut peers = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        peers.push(PeerConfig {
            id: index as u64 + 1,
            raft_address: format!("{CLUSTER_TEST_ADDR}:{}", ports[index]),
            client_submit_address: listener.local_addr().unwrap().to_string(),
        });
        // Each proxy and its connection tasks are cancelled and joined below.
        proxies.push(tokio::spawn(serve_proxy(
            listener,
            ports[index + 3],
            legacy.clone(),
            events.clone(),
        )));
    }
    drop(events);
    let vips: Vec<_> = (1..=3)
        .map(|last| VipConfig {
            address: VipAddr::host(ip4(192, 0, 2, last)),
            interface: "lo".into(),
            vlan: None,
        })
        .collect();
    let directory = std::env::temp_dir().join(format!(
        "keepafloatd-health-proof-{}-{}",
        std::process::id(),
        ports[0],
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let mut locals = Vec::new();
    let mut stops = Vec::new();
    let mut daemons = Vec::new();
    for index in 0..3 {
        let script = directory.join(format!("notify-{index}.sh"));
        tokio::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s %s %s\\n' \"$1\" \"$2\" \"$3\" >> \"$0.log\"\n",
        )
        .await
        .unwrap();
        tokio::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();
        let flag = directory.join(format!("healthy-{index}"));
        tokio::fs::write(&flag, b"").await.unwrap();
        let mut cfg = (*make_cfg(index, &peers, &vips)).clone();
        cfg.client_submit_listen = format!("{CLUSTER_TEST_ADDR}:{}", ports[index + 3]);
        cfg.health.command = vec![
            "/bin/sh".into(),
            "-c".into(),
            "test -f \"$1\"".into(),
            "health".into(),
            flag.to_str().unwrap().into(),
        ];
        cfg.notify = Some(script.to_str().unwrap().into());
        // Execute real notify scripts, while LocalVip keeps all address effects dry-run.
        cfg.dry_run = false;
        let cfg = Arc::new(cfg);
        let local = LocalVip::new(true);
        let (stop, stopped) = oneshot::channel();
        // The test always stops and joins the real daemon before removing its fixtures.
        daemons.push(tokio::spawn(run(
            cfg.clone(),
            Arc::new(cfg.sorted_vips()),
            local.clone(),
            async {
                let _ = stopped.await;
            },
        )));
        locals.push(local);
        stops.push(stop);
    }

    let outcome = tokio::time::timeout(Duration::from_secs(4), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            let mut sizes = Vec::new();
            for local in &locals {
                sizes.push(local.bound_addrs().await.len());
            }
            if sizes == [1, 1, 1] && legacy.follower.load(Ordering::SeqCst) != 0 {
                break;
            }
        }
        let node = legacy.follower.load(Ordering::SeqCst);
        let index = node as usize - 1;
        let vip = locals[index]
            .bound_addrs()
            .await
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("follower lost its VIP before proof rejection"))?;
        let log = directory.join(format!("notify-{index}.sh.log"));
        let mut expected = loop {
            poll.tick().await;
            let states = notifications(&log, vip).await?;
            anyhow::ensure!(!states.iter().any(|state| state == "FAULT"));
            if states.last().is_some_and(|state| state == "MASTER") {
                break states;
            }
        };
        legacy.fenced_node.store(node, Ordering::SeqCst);
        anyhow::ensure!(received.recv().await == Some(LegacyEvent::ProofRejected));
        anyhow::ensure!(received.recv().await == Some(LegacyEvent::UnhealthyFallback));
        expected.push("BACKUP".into());
        wait_notifications(&log, vip, &expected).await?;
        anyhow::ensure!(
            locals[index].bound_addrs().await.is_empty(),
            "fenced VIP remained bound"
        );
        anyhow::ensure!(!daemons[index].is_finished(), "fencing stopped the daemon");

        legacy.acknowledge.add_permits(1);
        // Observe the first ACK and route the next fallback before restoring proof support.
        let (mut acknowledged, mut rejected, mut fallback) = (false, false, false);
        while !(acknowledged && rejected && fallback) {
            match received.recv().await {
                Some(LegacyEvent::Acknowledged) => acknowledged = true,
                Some(LegacyEvent::ProofRejected) => rejected = true,
                Some(LegacyEvent::UnhealthyFallback) => fallback = true,
                None => anyhow::bail!("legacy submit proxy stopped"),
            }
        }
        legacy.fenced_node.store(0, Ordering::SeqCst);
        expected.push("MASTER".into());
        wait_notifications(&log, vip, &expected).await?;
        anyhow::ensure!(locals[index].bound_addrs().await.contains(&vip));
        tokio::fs::remove_file(directory.join(format!("healthy-{index}"))).await?;
        expected.push("FAULT".into());
        wait_notifications(&log, vip, &expected).await?;
        anyhow::ensure!(locals[index].bound_addrs().await.is_empty());
        Ok::<(), anyhow::Error>(())
    })
    .await;

    for proxy in &proxies {
        proxy.abort();
    }
    for stop in stops {
        let _ = stop.send(());
    }
    let mut daemon_results = Vec::new();
    for daemon in daemons {
        daemon_results.push(join_daemon(daemon).await);
    }
    let mut proxy_results = Vec::new();
    for proxy in proxies {
        proxy_results.push(proxy.await);
    }
    let cleanup = remove_fixtures(&directory).await;
    for result in daemon_results {
        assert_eq!(result.unwrap(), None);
    }
    for result in proxy_results {
        assert!(
            matches!(&result, Err(error) if error.is_cancelled()),
            "proxy failed: {result:?}"
        );
    }
    outcome.expect("health-proof scenario timed out").unwrap();
    cleanup.unwrap();
}
