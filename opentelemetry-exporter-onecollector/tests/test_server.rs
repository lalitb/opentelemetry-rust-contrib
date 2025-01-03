use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use serde_json::Value;
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
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

        let make_svc = make_service_fn(move |_conn| {
            let logs_state = logs_state.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let logs_state = logs_state.clone();
                    async move {
                        if req.method() == hyper::Method::POST && req.uri().path() == "/logs" {
                            let body = hyper::body::to_bytes(req.into_body()).await?;
                            if let Ok(payload) = serde_json::from_slice::<Value>(&body) {
                                println!("Received payload: {}", payload);
                                let mut logs = logs_state.lock().unwrap();
                                logs.push(payload);
                                Ok::<_, Infallible>(Response::new(Body::from("OK")))
                            } else {
                                Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(400)
                                        .body(Body::from("Invalid JSON"))
                                        .unwrap(),
                                )
                            }
                        } else {
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(404)
                                    .body(Body::from("Not Found"))
                                    .unwrap(),
                            )
                        }
                    }
                }))
            }
        });

        let server = Server::bind(&address).serve(make_svc);

        let server_handle = tokio::spawn(async move {
            server.await.unwrap();
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
