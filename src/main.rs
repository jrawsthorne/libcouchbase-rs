use futures::SinkExt;
use rand::random;
use std::{
    collections::HashMap,
    convert::TryInto,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
    time::Duration,
    usize,
};
use tokio::{net::TcpStream, sync::oneshot, time::sleep};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

mod frame;
use frame::{Codec, DataType, Feature, Frame, Status};

pub type Opaque = u32;
pub struct Connection {
    out_tx: tokio::sync::mpsc::UnboundedSender<Frame>,
    state: Mutex<ConnectionState>,
    addr: SocketAddr,
}

#[derive(Default)]
pub(crate) struct ConnectionState {
    response_map: HashMap<Opaque, oneshot::Sender<Frame>>,
    cluster_config: Option<ClusterConfig>,
    connected: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    rev: u32,
    nodes_ext: Vec<NodeExt>,
    cluster_capabilities_ver: Vec<u32>,
    cluster_capabilities: HashMap<String, Vec<String>>,
    bucket_capabilities_ver: Option<String>,
    bucket_capabilities: Option<Vec<String>>,
    name: Option<String>,
    uri: Option<String>,
    streaming_uri: Option<String>,
    nodes: Option<Vec<Node>>,
    node_locator: Option<String>,
    uuid: Option<String>,
    ddocs: Option<HashMap<String, String>>,
    v_bucket_server_map: Option<VBucketServerMap>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeExt {
    services: HashMap<String, u16>,
    #[serde(default)]
    this_node: bool,
    hostname: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VBucketServerMap {
    hash_algorithm: String,
    num_replicas: u32,
    server_list: Vec<String>,
    v_bucket_map: Vec<Vec<i32>>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    couch_api_base: String,
    hostname: Option<String>,
    ports: HashMap<String, u16>,
}

pub struct Cluster {
    connections: RwLock<HashMap<SocketAddr, Arc<Connection>>>,
    cluster_config: RwLock<ClusterConfig>,
    username: String,
    password: String,
    bucket: Mutex<Option<String>>,
}

impl Cluster {
    pub async fn new(addr: SocketAddr, username: String, password: String) -> Self {
        let mut connections = HashMap::new();
        let bootstrap_connection = Connection::new(addr, username.clone(), password.clone()).await;
        connections.insert(addr, bootstrap_connection.clone());

        let cluster_config = bootstrap_connection.get_cluster_config().await;

        for node in &cluster_config.nodes_ext {
            let kv_port = node.services["kv"];
            let addr = format!("{}:{}", node.hostname.as_ref().unwrap(), kv_port)
                .parse()
                .unwrap();
            let connection = Connection::new(addr, username.clone(), password.clone()).await;
            connections.insert(addr, connection);
        }

        let cluster_config = RwLock::new(cluster_config);
        let connections = RwLock::new(connections);

        Self {
            connections,
            cluster_config,
            username,
            password,
            bucket: Default::default(),
        }
    }

    pub async fn open_bucket(&self, bucket: impl Into<String>) {
        let bucket = bucket.into();
        for connection in self.connections.read().unwrap().values() {
            connection.open_bucket(&bucket).await;
        }

        let connections = self.connections.read().unwrap();

        // get new cluster config from random node
        let random_node = connections.values().next().unwrap();
        let state = random_node.state.lock().unwrap();
        let new_config = state.cluster_config.as_ref().unwrap().clone();

        let old_config = &mut *self.cluster_config.write().unwrap();
        *old_config = new_config;

        *self.bucket.lock().unwrap() = Some(bucket);
    }

    fn cluster_config(&self) -> RwLockReadGuard<ClusterConfig> {
        self.cluster_config.read().unwrap()
    }

    fn connection_for_key(
        &self,
        cluster_config: &ClusterConfig,
        vbucket_id: u16,
    ) -> Arc<Connection> {
        let server_map = cluster_config.v_bucket_server_map.as_ref().unwrap();

        // Just get active for now
        let node_idx: u32 = server_map.v_bucket_map[vbucket_id as usize][0]
            .try_into()
            .unwrap();
        let node = &server_map.server_list[node_idx as usize];

        // TODO: just use str
        let addr = node.parse().unwrap();
        let connection = self.connections.read().unwrap()[&addr].clone();
        connection
    }

    pub async fn get<V: serde::de::DeserializeOwned, K: Into<String>>(
        &self,
        key: K,
    ) -> Result<Option<V>, Status> {
        let key = key.into();

        loop {
            let (connection, vbucket_id) = self.connection_and_vbucket_id(&key);
            println!("get {} from node {:?}", key, connection.addr);
            match connection.get(vbucket_id, key.clone()).await {
                Ok(val) => return Ok(val),
                Err(Status::NotMyVBucket) => {
                    self.handle_not_my_vbucket().await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn handle_not_my_vbucket(&self) {
        let mut updated_cluster_config = false;

        let connections: Vec<Arc<Connection>> =
            self.connections.read().unwrap().values().cloned().collect();

        for connection in connections {
            if !connection.state.lock().unwrap().connected {
                continue;
            }

            // TODO: Handle timeout/error
            connection.handle_config_change().await;

            if updated_cluster_config {
                continue;
            }

            let new_config = connection
                .state
                .lock()
                .unwrap()
                .cluster_config
                .clone()
                .unwrap();

            *self.cluster_config.write().unwrap() = new_config;

            let mut connections = self.connections.write().unwrap();

            // any new connections needed
            for node in &self.cluster_config.read().unwrap().nodes_ext {
                let kv_port = node.services["kv"];
                let addr = format!("{}:{}", node.hostname.as_ref().unwrap(), kv_port)
                    .parse()
                    .unwrap();

                if connections.contains_key(&addr) {
                    continue;
                }

                let connection =
                    Connection::new(addr, self.username.clone(), self.password.clone()).await;

                if let Some(bucket) = &*self.bucket.lock().unwrap() {
                    connection.open_bucket(bucket).await;
                }

                connections.insert(addr, connection);
            }

            updated_cluster_config = true;
        }

        if !updated_cluster_config {
            todo!("no connected nodes");
        }
    }

    fn connection_and_vbucket_id(&self, key: &str) -> (Arc<Connection>, u16) {
        let cluster_config = self.cluster_config();
        let vbucket_id = self.vbucket_id(&cluster_config, &key);
        let connection = self.connection_for_key(&cluster_config, vbucket_id);

        assert!(connection.state.lock().unwrap().connected);

        (connection, vbucket_id)
    }

    pub async fn set(
        &self,
        key: impl Into<String>,
        value: impl serde::ser::Serialize + std::fmt::Debug,
    ) -> Result<(), Status> {
        let key = key.into();

        loop {
            let (connection, vbucket_id) = self.connection_and_vbucket_id(&key);

            println!(
                "insert {}->{:?} into node {:?}",
                key, value, connection.addr
            );
            // TODO: Remove clone in happy case
            match connection.set(vbucket_id, key.clone(), &value).await {
                Ok(()) => return Ok(()),
                Err(Status::NotMyVBucket) => {
                    self.handle_not_my_vbucket().await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn delete(&self, key: impl Into<String>) -> Result<(), Status> {
        let key = key.into();

        loop {
            let (connection, vbucket_id) = self.connection_and_vbucket_id(&key);

            println!("delete {} from node {:?}", key, connection.addr);
            // TODO: Remove clone in happy case
            match connection.delete(vbucket_id, key.clone()).await {
                Ok(()) => return Ok(()),
                Err(Status::NotMyVBucket) => {
                    self.handle_not_my_vbucket().await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn vbucket_id(&self, cluster_config: &ClusterConfig, key: &str) -> u16 {
        let v_bucket_server_map = cluster_config.v_bucket_server_map.as_ref().unwrap();
        let v_bucket_id = v_bucket_hash(&key, v_bucket_server_map.v_bucket_map.len() as u32);
        v_bucket_id as u16
    }
}

impl Connection {
    pub async fn new(addr: SocketAddr, username: String, password: String) -> Arc<Connection> {
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let connection = Arc::new(Connection {
            state: Default::default(),
            out_tx,
            addr,
        });
        tokio::spawn(read_write_task(connection.clone(), out_rx));

        connection.init(username, password).await;

        connection
    }

    async fn init(&self, username: String, password: String) {
        self.handshake().await;
        let sasl_mechanisms = self.get_sasl_mechanisms().await;
        assert!(sasl_mechanisms.iter().any(|mech| mech == "PLAIN"));
        self.sasl_auth(username, password).await;
    }

    async fn get_cluster_config(&self) -> ClusterConfig {
        let resp = self
            .request(Frame::get_cluster_config_request())
            .await
            .unwrap();
        assert_eq!(resp.data_type, DataType::Json);
        serde_json::from_slice(&resp.value).expect("TODO: handle invalid json")
    }

    async fn handshake(&self) {
        let frame = Frame::hello_request(
            "Test".to_string().to_string(),
            vec![
                Feature::TcpNodelay,
                Feature::Xattr,
                Feature::Json,
                Feature::MutationSeqno,
                Feature::Datatype,
            ],
        );
        let _response = self.request(frame).await.unwrap();
    }

    async fn get_sasl_mechanisms(&self) -> Vec<String> {
        let response = self.request(Frame::sasl_list_mech_request()).await.unwrap();
        String::from_utf8(response.value.to_vec())
            .expect("TODO: check utf8")
            .split(' ')
            .map(str::to_string)
            .collect()
    }

    async fn sasl_auth(&self, username: String, password: String) {
        self.request(Frame::sasl_auth_request(username, password))
            .await
            .unwrap();
    }

    async fn request(&self, mut frame: Frame) -> Result<Frame, std::io::Error> {
        let opaque = random();
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().response_map.insert(opaque, tx);
        frame.opaque = opaque;
        self.out_tx.send(frame).unwrap();
        let response = rx.await.unwrap();

        Ok(response)
    }

    async fn open_bucket(&self, bucket: impl Into<String>) {
        let bucket = bucket.into();
        self.select_bucket(bucket).await;
        self.handle_config_change().await;
    }

    async fn handle_config_change(&self) {
        let cluster_config = self.get_cluster_config().await;
        self.state.lock().unwrap().cluster_config = Some(cluster_config);
    }

    async fn select_bucket(&self, bucket: String) {
        self.request(Frame::select_bucket_request(bucket))
            .await
            .unwrap();
    }

    async fn get<V: serde::de::DeserializeOwned>(
        &self,
        vbucket_id: u16,
        key: String,
    ) -> Result<Option<V>, Status> {
        let resp = self
            .request(Frame::get_request(key, vbucket_id))
            .await
            .unwrap();

        match resp.status.unwrap() {
            Status::Success => Ok(Some(serde_json::from_slice(&resp.value).unwrap())),
            Status::KeyNotFound => Ok(None),
            err => Err(err),
        }
    }

    async fn set(
        &self,
        vbucket_id: u16,
        key: String,
        value: impl serde::ser::Serialize,
    ) -> Result<(), Status> {
        let resp = self
            .request(Frame::set_request(key, value, vbucket_id))
            .await
            .unwrap();

        match resp.status.unwrap() {
            Status::Success => Ok(()),
            err => Err(err),
        }
    }

    async fn delete(&self, vbucket_id: u16, key: String) -> Result<(), Status> {
        let resp = self
            .request(Frame::delete_request(key, vbucket_id))
            .await
            .unwrap();

        match resp.status.unwrap() {
            Status::Success => Ok(()),
            err => Err(err),
        }
    }
}

async fn read_write_task(
    connection: Arc<Connection>,
    mut out_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
) {
    let mut stream = TcpStream::connect(connection.addr).await.unwrap();

    connection.state.lock().unwrap().connected = true;

    stream.set_nodelay(true).unwrap();
    let (r, w) = stream.split();
    let codec = Codec::new();
    let mut reader = FramedRead::new(r, codec);
    let mut writer = FramedWrite::new(w, codec);

    let read_task = async {
        while let Some(frame) = reader.next().await {
            let frame = frame.unwrap();
            if let Some(sender) = connection
                .state
                .lock()
                .unwrap()
                .response_map
                .remove(&frame.opaque)
            {
                let _ = sender.send(frame);
            }
        }
    };

    let write_task = async {
        while let Some(frame) = out_rx.recv().await {
            writer.send(frame).await.unwrap();
        }
    };

    let disconnected_task = async {
        loop {
            sleep(Duration::from_secs(10)).await;
            if !connection.state.lock().unwrap().connected {
                break;
            }
        }
    };

    tokio::select! {
        _ = read_task => {}
        _ = write_task => {}
        _ = disconnected_task => {}
    }

    connection.state.lock().unwrap().connected = false;
}

#[tokio::main]
async fn main() {
    let cluster = Cluster::new(
        "10.145.210.101:11210".parse().unwrap(),
        "Administrator".to_string(),
        "password".to_string(),
    )
    .await;

    cluster.open_bucket("travel-sample").await;

    loop {
        let id = random();
        let new_airline = Airline {
            id,
            r#type: "airline".to_string(),
            name: "Test".to_string(),
            iata: "Test".to_string(),
            icao: "Test".to_string(),
            callsign: "Test".to_string(),
            country: "Test".to_string(),
        };
        let key = format!("airline_{}", id);

        cluster.set(key.clone(), new_airline.clone()).await.unwrap();

        println!("inserted {:?} with key {}", new_airline, key);

        let airline: Airline = cluster.get(&key).await.unwrap().unwrap();

        assert_eq!(new_airline, airline);

        cluster.delete(&key).await.unwrap();

        assert!(cluster.get::<Airline, _>(&key).await.unwrap().is_none());

        sleep(Duration::from_secs(1)).await;
    }
}

fn v_bucket_hash(key: &str, num_vbuckets: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key.as_bytes());
    let crc = hasher.finalize();
    let hash = (((crc) >> 16) & 0x7fff) & (num_vbuckets - 1);
    hash
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Airline {
    id: u32,
    r#type: String,
    name: String,
    iata: String,
    icao: String,
    callsign: String,
    country: String,
}
