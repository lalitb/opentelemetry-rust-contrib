
// Test using:
// curl -X POST -H "Content-Type: application/json" -d '{"message": "Test log"}' http://127.0.0.1:3200/logs


use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A struct representing the test server.
pub struct TestServer {
    received_logs: Arc<Mutex<Vec<Value>>>,
    server_handle: Option<JoinHandle<()>>,
    address: SocketAddr,
}

impl TestServer {
    /// Creates and starts a new test server.
    pub fn new(port: u16) -> Self {
        let received_logs = Arc::new(Mutex::new(Vec::new()));
        let logs_state = received_logs.clone();
        let address = SocketAddr::from(([127, 0, 0, 1], port));

        let server_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(address).await.unwrap();
            println!("Listening on http://{}", address);

            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let logs_state = logs_state.clone();

                tokio::task::spawn(async move {
                    let io = TokioIo::new(stream);

                    if let Err(err) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let logs_state = logs_state.clone();
                                async move {
                                    match (req.method(), req.uri().path()) {
                                        (&Method::POST, "/logs") => {
                                            let body = req.collect().await?.to_bytes(); // Collect body
                                            let body_vec: Vec<u8> = body.to_vec(); // Convert to Vec<u8>

                                            if let Ok(payload) =
                                                serde_json::from_slice::<Value>(&body_vec)
                                            {
                                                println!("Received payload: {}", payload);
                                                logs_state.lock().unwrap().push(payload);
                                                Ok::<_, hyper::Error>(Response::new(full("OK")))
                                            } else {
                                                Ok::<_, hyper::Error>(
                                                    Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(full("Invalid JSON"))
                                                        .unwrap(),
                                                )
                                            }
                                        }
                                        _ => Ok::<_, hyper::Error>(
                                            Response::builder()
                                                .status(StatusCode::NOT_FOUND)
                                                .body(empty())
                                                .unwrap(),
                                        ),
                                    }
                                }
                            }),
                        )
                        .await
                    {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
        });

        Self {
            received_logs,
            server_handle: Some(server_handle),
            address,
        }
    }

    /// Returns the address of the server.
    pub fn address(&self) -> String {
        format!("http://{}/logs", self.address)
    }

    /// Retrieves received logs.
    pub fn get_logs(&self) -> Vec<Value> {
        self.received_logs.lock().unwrap().clone()
    }

    /// Stops the server.
    pub async fn stop(mut self) {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }
}

/// Helper function to create an empty body.
fn empty() -> BoxBody<Bytes, hyper::Error> {
    Full::<Bytes>::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed()
}

/// Helper function to create a full body from a string or bytes.
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[tokio::main]
async fn main() {
    // Create a new server on port 3000
    let server = TestServer::new(3200);
    
    println!("Server started at {}", server.address());

    // Keep the server running
    // You can use tokio::signal::ctrl_c() to handle graceful shutdown
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl+c");
        
    // Stop the server when ctrl+c is received
    server.stop().await;
}
