use super::{Daemon, Duration, fs, timeout};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn proof(secret: &str, role: u8, client: &[u8], server: &[u8]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(b"keepafloatd-mutual-auth\0");
    mac.update(&[3, role]);
    mac.update(client);
    mac.update(server);
    mac
}

async fn authenticated(address: SocketAddr, peer: u64, secret: &str) -> io::Result<TcpStream> {
    timeout(Duration::from_secs(1), async {
        let mut stream = TcpStream::connect(address).await?;
        let mut client = [0u8; 110];
        client[..8].copy_from_slice(b"KAFDAUTH");
        client[8..12].copy_from_slice(&[3, 1, 1, 7]);
        client[12..20].copy_from_slice(&peer.to_be_bytes());
        client[20..28].copy_from_slice(&1u64.to_be_bytes());
        getrandom::fill(&mut client[28..60]).unwrap();
        stream.write_all(&client).await?;
        let mut server = [0u8; 110];
        stream.read_exact(&mut server).await?;
        assert_eq!(&server[..11], b"KAFDAUTH\x03\x01\x02");
        assert_eq!(&server[12..20], &1u64.to_be_bytes());
        assert_eq!(&server[20..28], &peer.to_be_bytes());
        let mut tag = [0u8; 32];
        stream.read_exact(&mut tag).await?;
        proof(secret, 2, &client, &server)
            .verify_slice(&tag)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "server proof mismatch"))?;
        stream
            .write_all(&proof(secret, 1, &client, &server).finalize().into_bytes())
            .await?;
        stream.read_exact(&mut tag).await?;
        proof(secret, 3, &client, &server)
            .verify_slice(&tag)
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "server confirmation mismatch")
            })?;
        Ok(stream)
    })
    .await
    .expect("authentication did not finish promptly")
}

async fn expect_closed(mut stream: TcpStream) {
    timeout(Duration::from_secs(1), async {
        let mut reply = Vec::new();
        match stream.read_to_end(&mut reply).await {
            Ok(_) => assert!(reply.is_empty(), "invalid request received a response"),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
        }
    })
    .await
    .expect("invalid connection was not rejected promptly");
}

fn frame(value: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).unwrap();
    let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&body);
    bytes
}

async fn rejected(address: SocketAddr, bytes: &[u8]) {
    timeout(Duration::from_secs(1), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(bytes).await.unwrap();
        stream.shutdown().await.unwrap();
        expect_closed(stream).await;
    })
    .await
    .expect("invalid connection was not rejected promptly");
}

async fn read_status(stream: &mut TcpStream) -> serde_json::Value {
    let len = stream.read_u32().await.unwrap();
    assert!(len < 4096, "status response exceeds fixture limit");
    let mut body = vec![0; len as usize];
    stream.read_exact(&mut body).await.unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.as_object().unwrap().len(), 1);
    let status = envelope["status"].clone();
    assert_eq!(status["initialized"], true);
    status
}

#[tokio::test]
async fn reconnecting_clients_cannot_flood_network_warnings() {
    let daemon = Daemon::start().await;
    let config: serde_yaml::Value =
        serde_yaml::from_str(&fs::read_to_string(daemon.dir.join("config.yaml")).unwrap()).unwrap();
    let raft: SocketAddr = config["raft_listen"].as_str().unwrap().parse().unwrap();
    let submit: SocketAddr = config["client_submit_listen"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let secret = config["cluster_secret"].as_str().unwrap();
    let invalid_submit = frame(&serde_json::json!({"invalid_request": true}));

    for _ in 0..20 {
        assert_eq!(
            authenticated(raft, 1, "wrong-secret")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        rejected(raft, &[0]).await;
        rejected(submit, &invalid_submit).await;
    }
    expect_closed(authenticated(raft, 999, secret).await.unwrap()).await;

    let mismatched_status = frame(&serde_json::json!({"status": {
        "probe_from": 1,
        "config_fingerprint": {"version": 0, "digest": vec![0u8; 32]},
    }}));
    for _ in 0..20 {
        timeout(Duration::from_secs(1), async {
            let mut stream = authenticated(raft, 1, secret).await.unwrap();
            stream.write_all(&mismatched_status).await.unwrap();
            read_status(&mut stream).await;
            let mut extra = Vec::new();
            stream.read_to_end(&mut extra).await.unwrap();
            assert!(extra.is_empty(), "mismatched status stream stayed open");
        })
        .await
        .expect("mismatched status was not answered and closed promptly");
    }

    let status = timeout(Duration::from_secs(1), async {
        let mut stream = authenticated(raft, 1, secret).await.unwrap();
        stream
            .write_all(&frame(&serde_json::json!({"status": {"probe_from": 1}})))
            .await
            .unwrap();
        read_status(&mut stream).await
    })
    .await
    .expect("valid status request stopped working after rejection burst");

    let mut held = Vec::new();
    timeout(Duration::from_secs(2), async {
        for _ in 0..8 {
            let mut stream = authenticated(raft, 1, secret).await.unwrap();
            stream
                .write_all(&frame(&serde_json::json!({"status": {
                    "probe_from": 1,
                    "config_fingerprint": status["config_fingerprint"],
                }})))
                .await
                .unwrap();
            read_status(&mut stream).await;
            held.push(stream);
        }
        for _ in 0..20 {
            expect_closed(authenticated(raft, 1, secret).await.unwrap()).await;
        }
    })
    .await
    .expect("authenticated source quota did not reject excess clients promptly");

    let logs = daemon.logs();
    assert!(!logs.contains(secret), "configured secret appeared in logs");
    assert_eq!(logs.matches("submit from ").count(), 1, "{logs}");
    assert_eq!(logs.matches("unknown peer_id 999").count(), 1, "{logs}");
    assert_eq!(logs.matches("handshake io:").count(), 1, "{logs}");
    assert_eq!(
        logs.matches("source authenticated connection limit reached")
            .count(),
        1,
        "{logs}"
    );
    assert_eq!(
        logs.matches("preflight answered then stream closed")
            .count(),
        1,
        "{logs}"
    );
    daemon.assert_shutdown("TERM", 0).await;
}
