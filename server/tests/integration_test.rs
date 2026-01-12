use andross_server::{AddrConfig, AndrossConfig, start_server};
use andross_service::kv::CommandRequest;
use andross_service::kv::kv_service_client::KvServiceClient;
use andross_service::parse_uri;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::Channel;

struct ServerHandle {
    client: KvServiceClient<Channel>,
    cancellation_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<andross_server::Result<()>>,
}

#[tokio::test]
async fn test_three_node_cluster() {
    const NUM_SERVERS: usize = 3;

    // Start all servers.

    let mut listeners = Vec::with_capacity(NUM_SERVERS);
    for _ in 0..NUM_SERVERS {
        let listener = TcpListener::bind("[::1]:0").await.unwrap();
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
        let config = AndrossConfig {
            id: node_id,
            addr_config: AddrConfig::TcpListener(listener),
            peers,
            raft_tick_interval: Duration::from_millis(1),
            default_request_timeout: Duration::from_secs(5),
            cancellation_token: cancellation_token.clone(),
        };

        let join_handle = start_server(config).await.unwrap();

        let uri = parse_uri(&addr.to_string()).unwrap();
        let client = KvServiceClient::connect(uri).await.unwrap();

        let server = ServerHandle {
            client,
            cancellation_token,
            join_handle,
        };
        servers.push(server);
    }

    // Send a command to the cluster.

    let data = bytes::Bytes::from("hello");
    let request = CommandRequest { data };

    let mut success = false;
    for _ in 0..500 {
        for ServerHandle { client, .. } in &mut servers {
            if client.command(Request::new(request.clone())).await.is_ok() {
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
