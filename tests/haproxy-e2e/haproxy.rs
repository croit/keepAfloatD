use std::env;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: haproxy <listen-addr> <upstream-addr>");
        std::process::exit(2);
    }

    let listen_addr = args[1].clone();
    let upstream_addr = args[2].clone();
    let listener = TcpListener::bind(&listen_addr)?;

    eprintln!("haproxy stub listening on {listen_addr}, forwarding to {upstream_addr}");

    for incoming in listener.incoming() {
        match incoming {
            Ok(client) => {
                let upstream_addr = upstream_addr.clone();
                thread::spawn(move || {
                    if let Err(err) = proxy_connection(client, &upstream_addr) {
                        eprintln!("haproxy stub connection error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("haproxy stub accept error: {err}"),
        }
    }

    Ok(())
}

fn proxy_connection(client: TcpStream, upstream_addr: &str) -> io::Result<()> {
    let upstream = TcpStream::connect(upstream_addr)?;
    client.set_nodelay(true)?;
    upstream.set_nodelay(true)?;

    let mut client_read = client.try_clone()?;
    let mut client_write = client;
    let mut upstream_read = upstream.try_clone()?;
    let mut upstream_write = upstream;

    let upstream_to_client = thread::spawn(move || io::copy(&mut upstream_read, &mut client_write));
    let client_to_upstream = thread::spawn(move || io::copy(&mut client_read, &mut upstream_write));

    let _ = client_to_upstream.join();
    let _ = upstream_to_client.join();
    Ok(())
}
