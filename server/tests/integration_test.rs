use andross_server::service::kv_service_client::KvServiceClient;
use andross_server::service::{CommandRequest, CommandResponse};
use andross_server::util::{any_port_listener, parse_uri};
use andross_server::{AddrConfig, AndrossConfig, Command, start_server, test_database};
use raft::storage::MemStorage;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::Channel;

#[cfg(test)]
mod turmoil_tests {
    use super::*;
    use hyper_util::rt::TokioIo;
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;
    use turmoil::Builder;

    #[test]
    fn test_concurrent_reads_and_writes_with_faults() {
        let mut sim = Builder::new()
            .tick_duration(Duration::from_millis(10))
            .simulation_duration(Duration::from_secs(30))
            .enable_tokio_io()
            .build();

        // Add hosts to simulation
        let num_servers = 3;
        let hostnames: Vec<_> = (1..=num_servers as u64)
            .map(|i| (i, format!("server-{i}")))
            .collect();

        let peers = Arc::new(Mutex::new(HashMap::new()));
        let initialized = Arc::new(Notify::new());

        for (node_id, hostname) in &hostnames {
            let peers = Arc::clone(&peers);
            let initialized = Arc::clone(&initialized);
            let node_id = *node_id;
            let hostname = hostname.clone();

            sim.host(hostname, move || {
                let peers = Arc::clone(&peers);
                let initialized = Arc::clone(&initialized);
                async move {
                    let listener = any_port_listener().await.unwrap();
                    let uri = parse_uri(&listener.local_addr().unwrap().to_string()).unwrap();

                    // Wait for the addresses of all the nodes to be created.
                    let peers = {
                        let mut guard = peers.lock().unwrap();
                        guard.insert(node_id, uri);
                        initialized.notify_waiters();
                        while guard.len() < num_servers {
                            drop(guard);
                            initialized.notified().await;
                            guard = peers.lock().unwrap();
                        }
                        guard.clone()
                    };

                    let (db, _temp_dir) = test_database().await;
                    let cancellation_token = CancellationToken::new();

                    let config = AndrossConfig {
                        id: node_id,
                        addr_config: AddrConfig::TcpListener(listener),
                        peers: peers.into_iter().filter(|(id, _)| *id != node_id).collect(),
                        raft_tick_interval: Duration::from_millis(100),
                        default_request_timeout: Duration::from_secs(5),
                        log_storage: MemStorage::new(),
                        db,
                        cancellation_token: cancellation_token.clone(),
                    };

                    let join_handle = start_server(config).await.unwrap();

                    cancellation_token.cancelled().await;
                    join_handle.await.unwrap().unwrap();

                    Ok(())
                }
            });
        }

        // Add clients to simulation
        let num_clients = 3;
        for i in 1..=num_clients {
            let client_hostname = format!("client-{i}");
            let peers = Arc::clone(&peers);
            let initialized = Arc::clone(&initialized);
            sim.client(client_hostname, async move {
                // Wait for the addresses of all the nodes to be created.
                let peers = {
                    let mut guard = peers.lock().unwrap();
                    while guard.len() < num_servers {
                        drop(guard);
                        initialized.notified().await;
                        guard = peers.lock().unwrap();
                    }
                    guard.clone()
                };

                // Give servers some time to start and elect a leader
                tokio::time::sleep(Duration::from_secs(2)).await;

                for j in 0..10 {
                    let key = format!("client-{i}-key-{j}");
                    let value = format!("value-{j}");

                    let mut success = false;
                    for _ in 0..10 {
                        // Try connecting to any server (one might be the leader)
                        for (_node_id, uri) in &peers {
                            let hostname = uri.authority().unwrap().to_string();
                            let connector = tower::service_fn(move |_| {
                                let hostname = hostname.clone();
                                async move {
                                    let stream = turmoil::net::TcpStream::connect(hostname).await?;
                                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                                }
                            });
                            let channel = match Channel::builder(uri.clone())
                                .connect_with_connector(connector)
                                .await
                            {
                                Ok(c) => c,
                                Err(_) => continue,
                            };

                            let mut client = KvServiceClient::new(channel);
                            let command =
                                Command::insert(key.as_bytes(), value.as_bytes()).into_bytes();
                            let request = Request::new(CommandRequest {
                                command_bytes: command,
                            });

                            match client.command(request).await {
                                Ok(_) => {
                                    success = true;
                                    break;
                                }
                                Err(e) => {
                                    println!("Error inserting key {key}: {e}");
                                }
                            }
                        }
                        if success {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    assert!(success, "Client {i} failed to insert key {key}");

                    // Read it back
                    for (_node_id, uri) in &peers {
                        let hostname = uri.authority().unwrap().to_string();
                        let connector = tower::service_fn(move |_| {
                            let hostname = hostname.clone();
                            async move {
                                let stream = turmoil::net::TcpStream::connect(hostname).await?;
                                Ok::<_, std::io::Error>(TokioIo::new(stream))
                            }
                        });
                        let channel = match Channel::builder(uri.clone())
                            .connect_with_connector(connector)
                            .await
                        {
                            Ok(c) => c,
                            Err(_) => continue,
                        };

                        let mut client = KvServiceClient::new(channel);
                        let command = Command::read(key.as_bytes()).into_bytes();
                        let request = Request::new(CommandRequest {
                            command_bytes: command,
                        });

                        if let Ok(response) = client.command(request).await {
                            let response = response.into_inner();
                            assert_eq!(
                                response.response_bytes,
                                Some(value.as_bytes().to_vec().into())
                            );
                        }
                    }
                }

                Ok(())
            });
        }

        // Inject some faults while the simulation is running
        let server_hostnames: Vec<_> = hostnames
            .iter()
            .map(|(_node_id, hostname)| hostname.clone())
            .collect();
        sim.host("fault-injector", move || {
            let server_hostnames = server_hostnames.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(4)).await;

                // Simulate a network partition for server-1
                let s1 = server_hostnames[0].clone();
                let s2 = server_hostnames[1].clone();
                let s3 = server_hostnames[2].clone();

                println!("Injecting partition: {s1} is isolated");
                turmoil::partition(s1.clone(), s2.clone());
                turmoil::partition(s1.clone(), s3.clone());

                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(())
            }
        });

        sim.run().unwrap();
    }
}

struct ServerHandle {
    client: KvServiceClient<Channel>,
    cancellation_token: CancellationToken,
    _temp_dir: TempDir,
    join_handle: tokio::task::JoinHandle<andross_server::Result<()>>,
}

#[tokio::test]
async fn test_three_node_cluster() {
    const NUM_SERVERS: usize = 3;

    // Start all servers.
    let mut listeners = Vec::with_capacity(NUM_SERVERS);
    for _ in 0..NUM_SERVERS {
        let listener = any_port_listener().await.unwrap();
        listeners.push(listener);
    }
    let peers: HashMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(idx, listener)| {
            (
                idx as u64 + 1,
                parse_uri(&listener.local_addr().unwrap().to_string()).unwrap(),
            )
        })
        .collect();

    let mut servers = Vec::with_capacity(NUM_SERVERS);
    for (idx, listener) in listeners.into_iter().enumerate() {
        let node_id = idx as u64 + 1;
        let addr = listener.local_addr().unwrap();
        let peers = peers
            .clone()
            .into_iter()
            .filter(|(peer_id, _)| *peer_id != node_id)
            .collect();
        let cancellation_token = CancellationToken::new();
        let (db, temp_dir) = test_database().await;
        let config = AndrossConfig {
            id: node_id,
            addr_config: AddrConfig::TcpListener(listener),
            peers,
            raft_tick_interval: Duration::from_millis(1),
            default_request_timeout: Duration::from_secs(5),
            log_storage: MemStorage::new(),
            db,
            cancellation_token: cancellation_token.clone(),
        };

        let join_handle = start_server(config).await.unwrap();

        let uri = parse_uri(&addr.to_string()).unwrap();
        let client = KvServiceClient::connect(uri).await.unwrap();
        let server = ServerHandle {
            client,
            cancellation_token,
            _temp_dir: temp_dir,
            join_handle,
        };
        servers.push(server);
    }

    // Send an insert to the cluster.
    let mut success = false;
    let key = b"k0";
    let value = b"v0";
    let command_bytes = Command::insert(key, value).into_bytes();
    for _ in 0..500 {
        let request = CommandRequest {
            command_bytes: command_bytes.clone(),
        };
        for ServerHandle { client, .. } in &mut servers {
            if let Ok(response) = client.command(Request::new(request.clone())).await {
                let CommandResponse { response_bytes } = response.into_inner();
                assert_eq!(response_bytes, Some(Vec::new().into()));
                success = true;
                break;
            }
        }
        if success {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        success,
        "At least one node should have accepted the command"
    );

    // Read the value back from all nodes.
    for ServerHandle { client, .. } in &mut servers {
        let command_bytes = Command::read(key).into_bytes();
        let request = CommandRequest { command_bytes };
        let CommandResponse { response_bytes } = client
            .command(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response_bytes, Some(value.to_vec().into()));
    }

    for ServerHandle {
        cancellation_token,
        join_handle,
        ..
    } in servers
    {
        cancellation_token.cancel();
        join_handle.await.unwrap().unwrap();
    }
}
