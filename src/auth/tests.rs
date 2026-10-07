use super::*;

pub(crate) async fn server<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    secret: Option<&str>,
    listener: Listener,
) -> io::Result<Peer> {
    Ok(server_bound(stream, local, secret, listener).await?.peer)
}

async fn server_with_nonce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    secret: Option<&str>,
    listener: Listener,
    entropy: impl FnOnce() -> io::Result<[u8; 32]>,
) -> io::Result<Peer> {
    Ok(
        server_bound_with_nonce(stream, local, secret, listener, entropy)
            .await?
            .peer,
    )
}

#[tokio::test(start_paused = true)]
async fn version_two_short_hello_is_rejected_without_waiting_for_new_fields() {
    let (mut sender, mut receiver) = tokio::io::duplex(512);
    let mut old = hello(Peer::new(1, None, true), 2, Listener::Raft, 1, [1; 32]);
    old[8] = 2;
    sender.write_all(&old[..77]).await.unwrap();
    let started = tokio::time::Instant::now();
    let result = server(
        &mut receiver,
        Peer::new(2, None, true),
        Some(KEY),
        Listener::Raft,
    )
    .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(tokio::time::Instant::now(), started);
}

#[tokio::test]
async fn admission_channel_binding_is_shared_and_fresh() {
    let mut previous = None;
    for _ in 0..2 {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let (left, right) = tokio::join!(
            client_bound(
                &mut a,
                Peer::new(1, None, true),
                2,
                Some(KEY),
                Listener::Submit
            ),
            server_bound(
                &mut b,
                Peer::new(2, None, true),
                Some(KEY),
                Listener::Submit
            ),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left.binding, right.binding);
        assert_ne!(previous, Some(left.binding));
        previous = Some(left.binding);
    }
}

#[test]
fn admission_records_bind_exact_boots_role_and_payload() {
    use crate::raft::admission::ReplicaId;
    let from = ReplicaId {
        physical_id: u64::MAX,
        boot_nonce: [1; 32],
    };
    let to = ReplicaId {
        physical_id: 2,
        boot_nonce: [2; 32],
    };
    let body = b"nonce-bound-committed-progress";
    let tag = sign_admission_record(Some(KEY), 1, from, to, body).unwrap();
    verify_admission_record(Some(KEY), 1, from, to, body, &tag).unwrap();
    for (role, sender, target, bytes) in [
        (2, from, to, body.as_slice()),
        (
            1,
            ReplicaId {
                boot_nonce: [3; 32],
                ..from
            },
            to,
            body.as_slice(),
        ),
        (
            1,
            from,
            ReplicaId {
                boot_nonce: [3; 32],
                ..to
            },
            body.as_slice(),
        ),
        (1, from, to, b"different-request".as_slice()),
    ] {
        assert!(verify_admission_record(Some(KEY), role, sender, target, bytes, &tag).is_err());
    }
    assert!(verify_admission_record(None, 1, from, to, body, &tag).is_err());
}

#[test]
fn boot_admission_requires_incompatible_wire_version_three() {
    for listener in [Listener::Raft, Listener::Submit] {
        assert_eq!(
            hello(Peer::new(1, None, true), 2, listener, 1, [1; 32])[8],
            3,
            "boot-aware admission must not negotiate the previous protocol"
        );
    }
}

#[tokio::test]
async fn boot_nonce_is_authenticated_and_old_version_is_rejected() {
    let first = crate::raft::admission::ReplicaId {
        physical_id: 1,
        boot_nonce: [7; 32],
    };
    let second = crate::raft::admission::ReplicaId {
        physical_id: 2,
        boot_nonce: [8; 32],
    };
    let (mut client_io, mut server_io) = tokio::io::duplex(1024);
    let (client_result, server_result) = tokio::join!(
        client(
            &mut client_io,
            Peer::for_replica(first, None, true),
            2,
            Some(KEY),
            Listener::Submit
        ),
        server(
            &mut server_io,
            Peer::for_replica(second, None, true),
            Some(KEY),
            Listener::Submit
        ),
    );
    assert_eq!(client_result.unwrap().replica(), Some(second));
    assert_eq!(server_result.unwrap().replica(), Some(first));
    let mut old = hello(Peer::new(1, None, true), 2, Listener::Submit, 1, [1; 32]);
    old[8] = 2;
    assert!(
        read_hello(&mut old.as_slice(), Listener::Submit, 1, 2)
            .await
            .is_err()
    );
    let original = hello(
        Peer::for_replica(first, None, true),
        2,
        Listener::Submit,
        1,
        [1; 32],
    );
    let mut changed = original;
    changed[HELLO_BYTES - 1] ^= 1;
    let tag = mac(Some(KEY), 1, &original, b"reply")
        .unwrap()
        .finalize()
        .into_bytes();
    assert!(
        mac(Some(KEY), 1, &changed, b"reply")
            .unwrap()
            .verify_slice(&tag)
            .is_err()
    );
}

#[test]
fn tagged_wire_requires_authentication_version_three() {
    for listener in [Listener::Raft, Listener::Submit] {
        assert_eq!(
            hello(Peer::new(1, None, true), 2, listener, 1, [1; 32])[8],
            3
        );
    }
}

#[tokio::test]
async fn tagged_wire_rejects_version_one_in_both_directions_on_both_listeners() {
    for listener in [Listener::Raft, Listener::Submit] {
        for role in [1, 2] {
            let mut legacy = hello(Peer::new(1, None, true), 2, listener, role, [1; 32]);
            legacy[8] = 1;
            let result = read_hello(&mut legacy.as_slice(), listener, role, 2).await;
            assert!(
                result.is_err(),
                "version one accepted for {listener:?}, role {role}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn version_one_hellos_cannot_elicit_handshake_proofs() {
    for listener in [Listener::Raft, Listener::Submit] {
        let (mut sender, mut receiver) = tokio::io::duplex(512);
        let mut legacy = hello(Peer::new(1, None, true), 2, listener, 1, [1; 32]);
        legacy[8] = 1;
        sender.write_all(&legacy).await.unwrap();
        let error = server(&mut receiver, Peer::new(2, None, true), Some(KEY), listener)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid authentication version"));
        assert!(futures::poll!(Box::pin(sender.read_u8())).is_pending());

        let (mut client_stream, mut old_server) = tokio::io::duplex(512);
        let old_reply = async {
            let mut request = [0; HELLO_BYTES];
            old_server.read_exact(&mut request).await.unwrap();
            assert_eq!(request[8], 3);
            let mut response = hello(Peer::new(2, None, true), 1, listener, 2, [2; 32]);
            response[8] = 1;
            old_server.write_all(&response).await.unwrap();
        };
        let (result, ()) = tokio::join!(
            client(
                &mut client_stream,
                Peer::new(1, None, true),
                2,
                Some(KEY),
                listener
            ),
            old_reply,
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid authentication version")
        );
        assert!(futures::poll!(Box::pin(old_server.read_u8())).is_pending());
    }
}

struct Capture<S> {
    stream: S,
    written: Vec<u8>,
}

impl<S: AsyncRead + Unpin> AsyncRead for Capture<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Capture<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.stream).poll_write(cx, bytes);
        if let std::task::Poll::Ready(Ok(size)) = result {
            self.written.extend_from_slice(&bytes[..size]);
        }
        result
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn complete_wire_capture_contains_no_secret_and_nonces_are_fresh() {
    let mut previous = Vec::new();
    for _ in 0..2 {
        let (a, b) = tokio::io::duplex(512);
        let mut a = Capture {
            stream: a,
            written: Vec::new(),
        };
        let mut b = Capture {
            stream: b,
            written: Vec::new(),
        };
        let (first, second) = tokio::join!(
            client(
                &mut a,
                Peer::new(1, None, true),
                2,
                Some(KEY),
                Listener::Submit
            ),
            server(
                &mut b,
                Peer::new(2, None, true),
                Some(KEY),
                Listener::Submit
            )
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(a.written.len(), HELLO_BYTES + 32);
        assert_eq!(b.written.len(), HELLO_BYTES + 64);
        for bytes in [&a.written, &b.written] {
            assert!(!bytes.windows(KEY.len()).any(|part| part == KEY.as_bytes()));
        }
        assert_ne!(&a.written[28..60], &b.written[28..60]);
        assert_ne!(&a.written[28..60], previous);
        previous = a.written[28..60].to_vec();
    }
}

#[tokio::test]
async fn captured_client_proof_cannot_authenticate_a_new_server_nonce() {
    let old_client = hello(Peer::new(1, None, true), 2, Listener::Raft, 1, [1; 32]);
    let old_server = hello(Peer::new(2, None, true), 1, Listener::Raft, 2, [2; 32]);
    let old_proof = mac(Some(KEY), 1, &old_client, &old_server)
        .unwrap()
        .finalize()
        .into_bytes();
    let (mut a, mut b) = tokio::io::duplex(512);
    let attack = async {
        a.write_all(&old_client).await.unwrap();
        let mut reply = [0; HELLO_BYTES + 32];
        a.read_exact(&mut reply).await.unwrap();
        a.write_all(&old_proof).await.unwrap();
    };
    let (result, ()) = tokio::join!(
        server_with_nonce(
            &mut b,
            Peer::new(2, None, true),
            Some(KEY),
            Listener::Raft,
            || Ok([3; 32])
        ),
        attack
    );
    assert!(result.unwrap_err().to_string().contains("proof mismatch"));
    assert!(
        futures::poll!(Box::pin(a.read_u8())).is_pending(),
        "no final proof on failure"
    );
}

#[tokio::test]
async fn stale_server_proof_does_not_elicit_a_client_proof() {
    let old_client = hello(Peer::new(1, None, true), 2, Listener::Raft, 1, [1; 32]);
    let old_server = hello(Peer::new(2, None, true), 1, Listener::Raft, 2, [2; 32]);
    let (mut a, mut b) = tokio::io::duplex(512);
    let attack = async {
        let mut request = [0; HELLO_BYTES];
        b.read_exact(&mut request).await.unwrap();
        b.write_all(&old_server).await.unwrap();
        send_proof(&mut b, Some(KEY), 2, &old_client, &old_server)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(
        client_with_nonce(
            &mut a,
            Peer::new(1, None, true),
            2,
            Some(KEY),
            Listener::Raft,
            || Ok([3; 32])
        ),
        attack
    );
    assert!(result.unwrap_err().to_string().contains("proof mismatch"));
    assert!(futures::poll!(Box::pin(b.read_u8())).is_pending());
}

#[tokio::test]
async fn malformed_metadata_wrong_destination_and_cross_listener_fail_closed() {
    let valid = hello(Peer::new(1, None, true), 2, Listener::Raft, 1, [1; 32]);
    for (offset, value) in [(8, 0), (9, 2), (10, 2), (11, 0), (27, 3), (60, 2), (76, 1)] {
        let (mut a, mut b) = tokio::io::duplex(512);
        let mut bad = valid;
        bad[offset] = value;
        a.write_all(&bad).await.unwrap();
        assert!(
            server(&mut b, Peer::new(2, None, true), Some(KEY), Listener::Raft)
                .await
                .is_err()
        );
        assert!(futures::poll!(Box::pin(a.read_u8())).is_pending());
    }
}

#[tokio::test(start_paused = true)]
async fn server_entropy_failure_partial_hello_and_missing_client_proof_are_bounded() {
    let request = hello(Peer::new(1, None, true), 2, Listener::Raft, 1, [1; 32]);
    let (mut a, mut b) = tokio::io::duplex(512);
    a.write_all(&request).await.unwrap();
    assert!(
        server_with_nonce(
            &mut b,
            Peer::new(2, None, true),
            Some(KEY),
            Listener::Raft,
            || Err(io::Error::other("entropy unavailable"))
        )
        .await
        .is_err()
    );
    assert!(futures::poll!(Box::pin(a.read_u8())).is_pending());
    for prefix in [&request[..9], &request[..]] {
        let (mut a, mut b) = tokio::io::duplex(512);
        a.write_all(prefix).await.unwrap();
        assert_eq!(
            server(&mut b, Peer::new(2, None, true), Some(KEY), Listener::Raft)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }
}

const KEY: &str = "authentication-test-key-0123456789";

#[tokio::test]
async fn mutual_proofs_preserve_both_peer_metadata() {
    for listener in [Listener::Raft, Listener::Submit] {
        let (mut a, mut b) = tokio::io::duplex(512);
        let first = Peer::new(1, Some(15), false);
        let second = Peer::new(2, Some(18), true);
        let (client, server) = tokio::join!(
            client_with_nonce(&mut a, first, 2, Some(KEY), listener, || Ok([1; 32])),
            server_with_nonce(&mut b, second, Some(KEY), listener, || Ok([2; 32]))
        );
        assert_eq!(client.unwrap(), second);
        assert_eq!(server.unwrap(), first);
    }
}

#[test]
fn every_transcript_byte_and_proof_role_is_authenticated() {
    let a = hello(Peer::new(1, Some(9), true), 2, Listener::Raft, 1, [1; 32]);
    let b = hello(Peer::new(2, None, false), 1, Listener::Raft, 2, [2; 32]);
    let tag = mac(Some(KEY), 1, &a, &b).unwrap().finalize().into_bytes();
    for index in 0..HELLO_BYTES {
        let mut changed = a;
        changed[index] ^= 1;
        assert!(
            mac(Some(KEY), 1, &changed, &b)
                .unwrap()
                .verify_slice(&tag)
                .is_err()
        );
        let mut changed = b;
        changed[index] ^= 1;
        assert!(
            mac(Some(KEY), 1, &a, &changed)
                .unwrap()
                .verify_slice(&tag)
                .is_err()
        );
    }
    for role in [2, 3] {
        assert!(
            mac(Some(KEY), role, &a, &b)
                .unwrap()
                .verify_slice(&tag)
                .is_err()
        );
    }
    assert!(
        mac(Some("wrong-key"), 1, &a, &b)
            .unwrap()
            .verify_slice(&tag)
            .is_err()
    );
    assert!(mac(None, 1, &a, &b).is_err());
}

#[test]
fn rfc4231_hmac_sha256_known_answer() {
    let mut mac = Hmac::<Sha256>::new_from_slice(&[0x0b; 20]).unwrap();
    mac.update(b"Hi There");
    let actual = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(
        actual,
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[tokio::test(start_paused = true)]
async fn silent_peer_is_bounded_and_entropy_failure_writes_nothing() {
    let (mut a, mut b) = tokio::io::duplex(512);
    let error = client_with_nonce(
        &mut a,
        Peer::new(1, None, true),
        2,
        Some(KEY),
        Listener::Raft,
        || Err(io::Error::other("injected entropy failure")),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("entropy"));
    assert!(futures::poll!(Box::pin(b.read_u8())).is_pending());
    assert_eq!(
        client(
            &mut a,
            Peer::new(1, None, true),
            2,
            Some(KEY),
            Listener::Raft
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn legacy_hello_is_rejected_without_reply() {
    let (mut a, mut b) = tokio::io::duplex(512);
    a.write_all(&1u64.to_be_bytes()).await.unwrap();
    let error = server(&mut b, Peer::new(2, None, true), Some(KEY), Listener::Raft)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unsupported authentication"));
    assert!(futures::poll!(Box::pin(a.read_u8())).is_pending());
}

#[test]
fn transcript_matches_python_probe_golden_vector() {
    let c = hello(Peer::new(1, Some(9), true), 2, Listener::Raft, 1, [1; 32]);
    let s = hello(Peer::new(2, None, true), 1, Listener::Raft, 2, [2; 32]);
    let tag = mac(Some("test-key"), 1, &c, &s)
        .unwrap()
        .finalize()
        .into_bytes();
    let actual: String = tag.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        actual,
        "d7ea2643ac34c4c244ab7f3aef92bb05b505edd9267fca83286fe456050635f7"
    );
}

#[tokio::test]
async fn responder_identity_and_final_confirmation_are_required() {
    for wrong_identity in [false, true] {
        let (mut a, mut b) = tokio::io::duplex(512);
        let fake = async {
            let mut request = [0; HELLO_BYTES];
            b.read_exact(&mut request).await.unwrap();
            let response = hello(
                Peer::new(if wrong_identity { 3 } else { 2 }, None, true),
                1,
                Listener::Raft,
                2,
                [2; 32],
            );
            b.write_all(&response).await.unwrap();
            send_proof(&mut b, Some(KEY), 2, &request, &response)
                .await
                .unwrap();
            if !wrong_identity {
                verify_proof(&mut b, Some(KEY), 1, &request, &response)
                    .await
                    .unwrap();
                // Reflect the responder's proof where the distinct final role is required.
                send_proof(&mut b, Some(KEY), 2, &request, &response)
                    .await
                    .unwrap();
            }
        };
        let (result, ()) = tokio::join!(
            client(
                &mut a,
                Peer::new(1, None, true),
                2,
                Some(KEY),
                Listener::Raft
            ),
            fake
        );
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn absent_or_empty_keys_never_send_a_hello() {
    for key in [None, Some("")] {
        let (mut a, mut b) = tokio::io::duplex(512);
        assert!(
            client(&mut a, Peer::new(1, None, true), 2, key, Listener::Raft)
                .await
                .is_err()
        );
        assert!(futures::poll!(Box::pin(b.read_u8())).is_pending());
    }
}
