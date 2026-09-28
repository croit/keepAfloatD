use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::canonical_socket_addr;

/// Two-stage connection admission: one bound before authentication and another for work accepted
/// after authentication. Dropping either permit returns its capacity immediately.
#[derive(Clone)]
pub(crate) struct ConnectionAdmission {
    unauthenticated: Arc<Semaphore>,
    authenticated: Arc<Semaphore>,
}

impl ConnectionAdmission {
    pub(crate) fn new(unauthenticated_limit: usize, authenticated_limit: usize) -> Self {
        Self {
            unauthenticated: Arc::new(Semaphore::new(unauthenticated_limit)),
            authenticated: Arc::new(Semaphore::new(authenticated_limit)),
        }
    }

    pub(crate) fn try_begin(&self) -> Option<UnauthenticatedConnection> {
        self.unauthenticated
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| UnauthenticatedConnection {
                _permit: permit,
                authenticated: self.authenticated.clone(),
            })
    }
}

pub(crate) struct UnauthenticatedConnection {
    _permit: OwnedSemaphorePermit,
    authenticated: Arc<Semaphore>,
}

impl UnauthenticatedConnection {
    pub(crate) fn try_authenticate(self) -> Result<AuthenticatedConnection, Self> {
        match self.authenticated.clone().try_acquire_owned() {
            Ok(permit) => Ok(AuthenticatedConnection { _permit: permit }),
            Err(_) => Err(self),
        }
    }
}

pub(crate) struct AuthenticatedConnection {
    _permit: OwnedSemaphorePermit,
}

/// Shared cap on bytes reserved for authenticated inbound frame bodies. A permit is acquired from
/// the declared length before allocating and remains owned by the frame until dispatch drops it.
#[derive(Clone)]
pub(crate) struct FrameByteBudget {
    bytes: Arc<Semaphore>,
}

impl FrameByteBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(limit)),
        }
    }

    pub(crate) fn try_reserve(&self, bytes: u32) -> std::io::Result<FrameBytePermit> {
        if bytes == 0 {
            return Ok(FrameBytePermit { _permit: None });
        }
        self.bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map(|permit| FrameBytePermit {
                _permit: Some(permit),
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("in-flight frame byte budget exhausted by {bytes}-byte frame"),
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn available_bytes(&self) -> usize {
        self.bytes.available_permits()
    }
}

#[derive(Debug)]
pub(crate) struct FrameBytePermit {
    _permit: Option<OwnedSemaphorePermit>,
}

/// Connect from the IP advertised for this node instead of relying on the host's route-selected
/// source. Inbound identity checks can then reject spoofed peer IDs without breaking multihomed
/// nodes whose default egress address differs from their cluster address.
pub(crate) async fn connect_from_advertised(
    local_address: &str,
    remote_address: &str,
) -> anyhow::Result<tokio::net::TcpStream> {
    let mut local = canonical_socket_addr(local_address.parse()?);
    local.set_port(0);
    let remote = canonical_socket_addr(remote_address.parse()?);
    anyhow::ensure!(
        local.is_ipv4() == remote.is_ipv4(),
        "local advertised address {local_address} and remote address {remote_address} use different families"
    );
    let socket = if remote.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    socket.bind(local)?;
    Ok(socket.connect(remote).await?)
}

#[cfg(test)]
mod tests {
    use super::{ConnectionAdmission, connect_from_advertised};

    #[test]
    fn saturation_is_shed_and_capacity_recovers_after_permits_drop() {
        let admission = ConnectionAdmission::new(1, 1);

        let Some(first) = admission.try_begin() else {
            panic!("first unauthenticated connection should be admitted");
        };
        assert!(
            admission.try_begin().is_none(),
            "unauthenticated capacity must be bounded"
        );

        let Ok(authenticated) = first.try_authenticate() else {
            panic!("first authenticated connection should be admitted");
        };
        let Some(second) = admission.try_begin() else {
            panic!("authentication must release unauthenticated capacity");
        };
        let Err(rejected) = second.try_authenticate() else {
            panic!("authenticated capacity must be bounded independently");
        };
        drop(rejected);
        drop(authenticated);

        let Some(recovered) = admission.try_begin() else {
            panic!("unauthenticated capacity should recover after rejection");
        };
        assert!(
            recovered.try_authenticate().is_ok(),
            "authenticated capacity should recover after its permit drops"
        );
    }

    #[tokio::test]
    async fn outbound_connection_uses_the_advertised_source_ip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            connect_from_advertised("127.0.0.2:17110", &remote.to_string())
                .await
                .unwrap()
        });

        let (_server, observed) = listener.accept().await.unwrap();
        assert_eq!(
            observed.ip(),
            "127.0.0.2".parse::<std::net::IpAddr>().unwrap()
        );
        client.await.unwrap();
    }
}
