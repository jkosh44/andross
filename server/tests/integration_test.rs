use andross_server::service::kv_service_client::KvServiceClient;
use andross_server::service::{CommandRequest, CommandResponse};
use andross_server::{
    AddrConfig, AndrossConfig, Command, parse_uri, start_server, test_database,
    test_database_from_state,
};
use raft::storage::MemStorage;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::Channel;

struct ServerHandle {
    client: KvServiceClient<Channel>,
    cancellation_token: CancellationToken,
    temp_dir: TempDir,
    join_handle: tokio::task::JoinHandle<andross_server::Result<()>>,
}

impl ServerHandle {
    /// Wait until this server either becomes the leader or connects to the leader.
    async fn wait_for_leader(&mut self) {
        let command_bytes = Command::read(b"").into_bytes();
        let request = CommandRequest { command_bytes };
        while self
            .client
            .command(Request::new(request.clone()))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn test_three_node_cluster() {
    const NUM_SERVERS: usize = 3;

    // Start all servers.
    let mut servers = start_servers(ServerInitState::Num(NUM_SERVERS)).await;
    for server in &mut servers {
        server.wait_for_leader().await;
    }

    // Send an insert to the cluster.
    let key = b"k0";
    let value = b"v0";
    let command_bytes = Command::insert(key, value).into_bytes();
    let request = CommandRequest {
        command_bytes: command_bytes.clone(),
    };
    let CommandResponse { response_bytes } = servers
        .first_mut()
        .unwrap()
        .client
        .command(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_bytes, Some(Vec::new().into()));

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

    // Shut down all nodes.
    let mut temp_dirs = Vec::with_capacity(servers.len());
    for ServerHandle {
        cancellation_token,
        join_handle,
        temp_dir,
        ..
    } in servers
    {
        cancellation_token.cancel();
        join_handle.await.unwrap().unwrap();
        temp_dirs.push(temp_dir);
    }

    // Start them back up.
    let mut servers = start_servers(ServerInitState::FromState(temp_dirs)).await;
    for server in &mut servers {
        server.wait_for_leader().await;
    }

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

    // Shut down all nodes.
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

enum ServerInitState {
    Num(usize),
    FromState(Vec<TempDir>),
}

impl ServerInitState {
    fn len(&self) -> usize {
        match self {
            ServerInitState::Num(n) => *n,
            ServerInitState::FromState(state) => state.len(),
        }
    }
}

async fn start_servers(mut server_init_state: ServerInitState) -> Vec<ServerHandle> {
    let mut listeners = Vec::with_capacity(server_init_state.len());
    for _ in 0..server_init_state.len() {
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

    let mut servers = Vec::with_capacity(listeners.len());
    for (idx, listener) in listeners.into_iter().enumerate() {
        let node_id = idx as u64 + 1;
        let addr = listener.local_addr().unwrap();
        let peers = peers
            .clone()
            .into_iter()
            .filter(|(peer_id, _)| *peer_id != node_id)
            .collect();
        let cancellation_token = CancellationToken::new();
        let (db, temp_dir) = match &mut server_init_state {
            ServerInitState::Num(_) => test_database().await,
            ServerInitState::FromState(states) => {
                let temp_dir = states.remove(0);
                let db = test_database_from_state(temp_dir.path().to_path_buf()).await;
                (db, temp_dir)
            }
        };
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
            temp_dir,
            join_handle,
        };
        servers.push(server);
    }
    servers
}
