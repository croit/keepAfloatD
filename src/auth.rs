//! Mutual key-possession proof only: subsequent records remain plaintext and unauthenticated.

use hmac::{Hmac, Mac};
use sha2::Digest;
use sha2::Sha256;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"KAFDAUTH";
const VERSION: u8 = 3;
const HELLO_BYTES: usize = 110;
const DOMAIN: &[u8] = b"keepafloatd-mutual-auth\0";
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct AuthenticatedPeer {
    pub(crate) peer: Peer,
    pub(crate) binding: [u8; 32],
}

fn channel_binding(client: &[u8], server: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"keepafloatd-admission-channel\0");
    hash.update([VERSION]);
    hash.update(client);
    hash.update(server);
    hash.finalize().into()
}

fn admission_record_mac(
    secret: Option<&str>,
    role: u8,
    from: crate::raft::admission::ReplicaId,
    to: crate::raft::admission::ReplicaId,
    body: &[u8],
) -> io::Result<Hmac<Sha256>> {
    let secret = secret
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("admission authentication requires a cluster secret"))?;
    let mut result = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| invalid("invalid admission authentication key"))?;
    result.update(b"keepafloatd-admission-record\0");
    result.update(&[VERSION, 1, role]);
    for replica in [from, to] {
        result.update(&replica.physical_id.to_be_bytes());
        result.update(&replica.boot_nonce);
    }
    result.update(&(body.len() as u64).to_be_bytes());
    result.update(body);
    Ok(result)
}

pub(crate) fn sign_admission_record(
    secret: Option<&str>,
    role: u8,
    from: crate::raft::admission::ReplicaId,
    to: crate::raft::admission::ReplicaId,
    body: &[u8],
) -> io::Result<[u8; 32]> {
    Ok(admission_record_mac(secret, role, from, to, body)?
        .finalize()
        .into_bytes()
        .into())
}

pub(crate) fn verify_admission_record(
    secret: Option<&str>,
    role: u8,
    from: crate::raft::admission::ReplicaId,
    to: crate::raft::admission::ReplicaId,
    body: &[u8],
    tag: &[u8; 32],
) -> io::Result<()> {
    admission_record_mac(secret, role, from, to, body)?
        .verify_slice(tag)
        .map_err(|_| invalid("admission record authentication mismatch"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Listener {
    Raft = 1,
    Submit = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Peer {
    pub(crate) id: u64,
    pub(crate) epoch: Option<u128>,
    pub(crate) supports_v2: bool,
    pub(crate) boot_nonce: Option<[u8; 32]>,
}

impl Peer {
    pub(crate) fn new(id: u64, epoch: Option<u128>, supports_v2: bool) -> Self {
        Self {
            id,
            epoch,
            supports_v2,
            boot_nonce: None,
        }
    }

    pub(crate) fn for_replica(
        replica: crate::raft::admission::ReplicaId,
        epoch: Option<u128>,
        supports_v2: bool,
    ) -> Self {
        Self {
            id: replica.physical_id,
            epoch,
            supports_v2,
            boot_nonce: Some(replica.boot_nonce),
        }
    }

    pub(crate) fn replica(self) -> Option<crate::raft::admission::ReplicaId> {
        self.boot_nonce
            .map(|boot_nonce| crate::raft::admission::ReplicaId {
                physical_id: self.id,
                boot_nonce,
            })
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn nonce() -> io::Result<[u8; 32]> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| io::Error::other("authentication entropy unavailable"))?;
    Ok(bytes)
}

fn hello(
    peer: Peer,
    target: u64,
    listener: Listener,
    role: u8,
    nonce: [u8; 32],
) -> [u8; HELLO_BYTES] {
    let mut bytes = [0; HELLO_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8] = VERSION;
    bytes[9] = listener as u8;
    bytes[10] = role;
    // Every supported binary understands config preflight and cancellation-safe RPCs.
    bytes[11] = 6 | u8::from(peer.supports_v2);
    bytes[12..20].copy_from_slice(&peer.id.to_be_bytes());
    bytes[20..28].copy_from_slice(&target.to_be_bytes());
    bytes[28..60].copy_from_slice(&nonce);
    bytes[60] = u8::from(peer.epoch.is_some());
    bytes[61..77].copy_from_slice(&peer.epoch.unwrap_or_default().to_be_bytes());
    bytes[77] = u8::from(peer.boot_nonce.is_some());
    bytes[78..].copy_from_slice(&peer.boot_nonce.unwrap_or_default());
    bytes
}

async fn read_hello<S: AsyncRead + Unpin>(
    stream: &mut S,
    listener: Listener,
    role: u8,
    target: u64,
) -> io::Result<([u8; HELLO_BYTES], Peer)> {
    let mut bytes = [0; HELLO_BYTES];
    stream.read_exact(&mut bytes[..8]).await?;
    if &bytes[..8] != MAGIC {
        return Err(invalid(
            "unsupported authentication protocol; full-cluster upgrade required",
        ));
    }
    stream.read_exact(&mut bytes[8..9]).await?;
    if bytes[8] != VERSION {
        return Err(invalid(
            "invalid authentication version; full-cluster upgrade required",
        ));
    }
    stream.read_exact(&mut bytes[9..]).await?;
    if bytes[9] != listener as u8
        || bytes[10] != role
        || !matches!(bytes[11], 6 | 7)
        || bytes[60] > 1
        || bytes[77] > 1
    {
        return Err(invalid(
            "invalid authentication version, listener, role or capabilities",
        ));
    }
    let mut id = [0; 8];
    id.copy_from_slice(&bytes[12..20]);
    let mut destination = [0; 8];
    destination.copy_from_slice(&bytes[20..28]);
    if u64::from_be_bytes(destination) != target {
        return Err(invalid("authentication destination mismatch"));
    }
    let mut epoch = [0; 16];
    epoch.copy_from_slice(&bytes[61..77]);
    if bytes[60] == 0 && epoch != [0; 16] {
        return Err(invalid("noncanonical authentication epoch"));
    }
    let mut peer = Peer::new(
        u64::from_be_bytes(id),
        (bytes[60] == 1).then_some(u128::from_be_bytes(epoch)),
        bytes[11] & 1 != 0,
    );
    let mut boot_nonce = [0; 32];
    boot_nonce.copy_from_slice(&bytes[78..]);
    if bytes[77] == 0 && boot_nonce != [0; 32] {
        return Err(invalid("noncanonical authentication boot nonce"));
    }
    peer.boot_nonce = (bytes[77] == 1).then_some(boot_nonce);
    Ok((bytes, peer))
}

fn mac(secret: Option<&str>, role: u8, client: &[u8], server: &[u8]) -> io::Result<Hmac<Sha256>> {
    let secret = secret
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("authentication requires a cluster secret"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| invalid("invalid authentication key"))?;
    mac.update(DOMAIN);
    mac.update(&[VERSION, role]);
    mac.update(client);
    mac.update(server);
    Ok(mac)
}

async fn send_proof<S: AsyncWrite + Unpin>(
    stream: &mut S,
    secret: Option<&str>,
    role: u8,
    client: &[u8],
    server: &[u8],
) -> io::Result<()> {
    stream
        .write_all(&mac(secret, role, client, server)?.finalize().into_bytes())
        .await
}

async fn verify_proof<S: AsyncRead + Unpin>(
    stream: &mut S,
    secret: Option<&str>,
    role: u8,
    client: &[u8],
    server: &[u8],
) -> io::Result<()> {
    let mut proof = [0; 32];
    stream.read_exact(&mut proof).await?;
    mac(secret, role, client, server)?
        .verify_slice(&proof)
        .map_err(|_| invalid("authentication proof mismatch"))
}

pub(crate) async fn client<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    target: u64,
    secret: Option<&str>,
    listener: Listener,
) -> io::Result<Peer> {
    tokio::time::timeout(
        TIMEOUT,
        client_with_nonce(stream, local, target, secret, listener, nonce),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "authentication handshake timed out",
        )
    })?
}

async fn client_with_nonce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    target: u64,
    secret: Option<&str>,
    listener: Listener,
    entropy: impl FnOnce() -> io::Result<[u8; 32]>,
) -> io::Result<Peer> {
    Ok(
        client_bound_with_nonce(stream, local, target, secret, listener, entropy)
            .await?
            .peer,
    )
}

pub(crate) async fn client_bound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    target: u64,
    secret: Option<&str>,
    listener: Listener,
) -> io::Result<AuthenticatedPeer> {
    tokio::time::timeout(
        TIMEOUT,
        client_bound_with_nonce(stream, local, target, secret, listener, nonce),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "authentication handshake timed out",
        )
    })?
}

async fn client_bound_with_nonce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    target: u64,
    secret: Option<&str>,
    listener: Listener,
    entropy: impl FnOnce() -> io::Result<[u8; 32]>,
) -> io::Result<AuthenticatedPeer> {
    let request = hello(local, target, listener, 1, entropy()?);
    mac(secret, 1, &request, &[])?;
    stream.write_all(&request).await?;
    let (response, peer) = read_hello(stream, listener, 2, local.id).await?;
    if peer.id != target {
        return Err(invalid("authentication responder mismatch"));
    }
    verify_proof(stream, secret, 2, &request, &response).await?;
    send_proof(stream, secret, 1, &request, &response).await?;
    verify_proof(stream, secret, 3, &request, &response).await?;
    Ok(AuthenticatedPeer {
        peer,
        binding: channel_binding(&request, &response),
    })
}

pub(crate) async fn server_bound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    secret: Option<&str>,
    listener: Listener,
) -> io::Result<AuthenticatedPeer> {
    tokio::time::timeout(
        TIMEOUT,
        server_bound_with_nonce(stream, local, secret, listener, nonce),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "authentication handshake timed out",
        )
    })?
}

async fn server_bound_with_nonce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: Peer,
    secret: Option<&str>,
    listener: Listener,
    entropy: impl FnOnce() -> io::Result<[u8; 32]>,
) -> io::Result<AuthenticatedPeer> {
    let (request, peer) = read_hello(stream, listener, 1, local.id).await?;
    let response = hello(local, peer.id, listener, 2, entropy()?);
    mac(secret, 2, &request, &response)?;
    stream.write_all(&response).await?;
    send_proof(stream, secret, 2, &request, &response).await?;
    verify_proof(stream, secret, 1, &request, &response).await?;
    send_proof(stream, secret, 3, &request, &response).await?;
    Ok(AuthenticatedPeer {
        peer,
        binding: channel_binding(&request, &response),
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::server;
