use futures::SinkExt;
use rand::random;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpStream, sync::oneshot};
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

impl Connection {
    pub async fn new(addr: SocketAddr, username: String, password: String) -> Arc<Connection> {
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let connection = Arc::new(Connection {
            state: Default::default(),
            out_tx,
            addr: addr.into(),
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
        let cluster_config = self.get_cluster_config().await;
        self.state.lock().unwrap().cluster_config = Some(cluster_config);
    }

    async fn select_bucket(&self, bucket: String) {
        self.request(Frame::select_bucket_request(bucket))
            .await
            .unwrap();
    }

    fn vbucket_id(&self, key: &str) -> u16 {
        let state = self.state.lock().unwrap();
        let config = state.cluster_config.as_ref().unwrap();

        let v_bucket_server_map = config.v_bucket_server_map.as_ref().unwrap();
        let v_bucket_id = v_bucket_hash(&key, v_bucket_server_map.v_bucket_map.len() as u32);
        v_bucket_id as u16
    }

    async fn get<V: serde::de::DeserializeOwned>(&self, key: &str) -> Option<V> {
        let vbucket_id = self.vbucket_id(key);

        let resp = self
            .request(Frame::get_request(key.to_string(), vbucket_id))
            .await
            .unwrap();

        assert!(resp.is_success());

        if resp.status.unwrap() == Status::KeyNotFound {
            None
        } else {
            Some(serde_json::from_slice(&resp.value).unwrap())
        }
    }

    async fn set(&self, key: impl Into<String>, value: impl serde::ser::Serialize) {
        let key = key.into();
        let vbucket_id = self.vbucket_id(&key);

        let resp = self
            .request(Frame::set_request(key, value, vbucket_id))
            .await
            .unwrap();

        assert!(resp.is_success());
    }
}

async fn read_write_task(
    connection: Arc<Connection>,
    mut out_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
) {
    let mut stream = TcpStream::connect(connection.addr).await.unwrap();
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

    tokio::select! {
        _ = read_task => {}
        _ = write_task => {}
    }
}

#[tokio::main]
async fn main() {
    let connection = Connection::new(
        "192.168.99.101:11210".parse().unwrap(),
        "Administrator".to_string(),
        "password".to_string(),
    )
    .await;
    connection.open_bucket("travel-sample").await;

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

    connection.set(key.clone(), new_airline.clone()).await;

    println!("inserted {:?} with key {}", new_airline, key);

    let airline = connection.get::<Airline>(&key).await.unwrap();

    assert_eq!(new_airline, airline);
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
