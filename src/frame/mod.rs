use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{convert::TryFrom, io, usize};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
#[derive(Clone, Copy)]
pub struct Codec {}

impl Codec {
    pub fn new() -> Self {
        Self {}
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum Status {
    Success,
    KeyNotFound,
    KeyExists,
    ValueTooLarge,
    InvalidArguments,
    ItemNotStored,
    AuthenticationError,
    NotMyVBucket,
    BucketNotConnected,
    StaleAuthenticationContext,
    AuthenticationContinue,
    RollbackRequired,
    NoAccess,
    NodeInitializing,
    UnknownCommand,
    OutOfMemory,
    NotSupported,
    InternalError,
    Busy,
    TemporaryFailure,
}

impl From<Status> for u16 {
    fn from(status: Status) -> Self {
        match status {
            Status::Success => 0x0000,
            Status::KeyNotFound => 0x0001,
            Status::InvalidArguments => 0x0004,
            Status::AuthenticationError => 0x0020,
            Status::NotMyVBucket => 0x0007,
            Status::KeyExists => todo!(),
            Status::ValueTooLarge => todo!(),
            Status::ItemNotStored => todo!(),
            Status::BucketNotConnected => todo!(),
            Status::StaleAuthenticationContext => todo!(),
            Status::AuthenticationContinue => todo!(),
            Status::RollbackRequired => todo!(),
            Status::NoAccess => todo!(),
            Status::NodeInitializing => todo!(),
            Status::UnknownCommand => todo!(),
            Status::OutOfMemory => todo!(),
            Status::NotSupported => todo!(),
            Status::InternalError => todo!(),
            Status::Busy => todo!(),
            Status::TemporaryFailure => todo!(),
        }
    }
}

impl TryFrom<u16> for Status {
    type Error = DecodeError;

    fn try_from(status: u16) -> Result<Self, Self::Error> {
        Ok(match status {
            0x0000 => Status::Success,
            0x0001 => Status::KeyNotFound,
            0x0004 => Status::InvalidArguments,
            0x0007 => Status::NotMyVBucket,
            0x0020 => Status::AuthenticationError,
            _ => return Err(anyhow::anyhow!("invalid status").into()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Magic {
    RequestFromServer,
    ResponseFromServer,
    RequestFromClient,
    ResponseFromClient,
}

impl From<Magic> for u8 {
    fn from(magic: Magic) -> Self {
        match magic {
            Magic::RequestFromClient => 0x80,
            Magic::ResponseFromServer => 0x81,
            Magic::RequestFromServer => 0x82,
            Magic::ResponseFromClient => 0x83,
        }
    }
}

impl TryFrom<u8> for Magic {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0x80 => Magic::RequestFromClient,
            0x81 => Magic::ResponseFromServer,
            0x82 => Magic::RequestFromServer,
            0x83 => Magic::ResponseFromClient,
            _ => return Err(anyhow::anyhow!("invalid magic").into()),
        })
    }
}

impl Magic {
    pub fn is_response(&self) -> bool {
        match self {
            Magic::RequestFromServer | Magic::RequestFromClient => false,
            Magic::ResponseFromClient | Magic::ResponseFromServer => true,
        }
    }

    pub fn is_request(&self) -> bool {
        match self {
            Magic::RequestFromServer | Magic::RequestFromClient => true,
            Magic::ResponseFromClient | Magic::ResponseFromServer => false,
        }
    }
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub(crate) struct FrameBuilder {
    pub(crate) magic: Magic,
    pub(crate) opcode: Option<Opcode>,
    pub(crate) data_type: DataType,
    pub(crate) vbucket_id: Option<u16>,
    pub(crate) status: Option<Status>,
    pub(crate) opaque: u32,
    pub(crate) cas: u64,
    pub(crate) extras: Bytes,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl FrameBuilder {
    pub fn request() -> FrameBuilder {
        FrameBuilder {
            magic: Magic::RequestFromClient,
            opcode: None,
            data_type: DataType::Raw,
            vbucket_id: Some(0), // vbucket_id must be set for request
            status: None,
            opaque: 0,
            cas: 0,
            extras: Bytes::new(),
            key: Bytes::new(),
            value: Bytes::new(),
        }
    }

    pub fn opcode(mut self, opcode: Opcode) -> FrameBuilder {
        self.opcode = Some(opcode);
        self
    }

    pub fn extras(mut self, extras: Bytes) -> FrameBuilder {
        self.extras = extras;
        self
    }

    pub fn key(mut self, key: impl Into<Bytes>) -> FrameBuilder {
        self.key = key.into();
        self
    }

    pub fn value(mut self, value: Bytes) -> FrameBuilder {
        self.value = value;
        self
    }

    pub fn vbucket_id(mut self, vbucket_id: u16) -> FrameBuilder {
        self.vbucket_id = Some(vbucket_id);
        self
    }

    pub fn build(self) -> Frame {
        Frame {
            magic: self.magic,
            opcode: self.opcode.unwrap(),
            data_type: self.data_type,
            vbucket_id: self.vbucket_id,
            status: self.status,
            opaque: self.opaque,
            cas: self.cas,
            extras: self.extras,
            key: self.key,
            value: self.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub(crate) magic: Magic,
    pub(crate) opcode: Opcode,
    pub(crate) data_type: DataType,
    pub(crate) vbucket_id: Option<u16>,
    pub(crate) status: Option<Status>,
    pub(crate) opaque: u32,
    pub(crate) cas: u64,
    pub(crate) extras: Bytes,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl Frame {
    pub(crate) fn hello_request(user_agent: String, features: Vec<Feature>) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::Hello)
            .key(user_agent)
            .value({
                let mut bytes = BytesMut::with_capacity(features.len() * 2);
                for feature in features {
                    bytes.put_u16(feature.as_u16());
                }
                bytes.freeze()
            })
            .build()
    }

    pub(crate) fn sasl_list_mech_request() -> Frame {
        FrameBuilder::request().opcode(Opcode::SaslListMech).build()
    }

    pub(crate) fn sasl_auth_request(username: String, password: String) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::SaslAuth)
            .key("PLAIN")
            .value({
                let mut bytes = BytesMut::with_capacity(2 + username.len() + password.len());
                bytes.put_u8(0);
                bytes.put(username.as_bytes());
                bytes.put_u8(0);
                bytes.put(password.as_bytes());
                bytes.freeze()
            })
            .build()
    }

    pub(crate) fn get_cluster_config_request() -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::GetClusterConfig)
            .build()
    }

    pub(crate) fn select_bucket_request(bucket: String) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::SelectBucket)
            .key(bucket)
            .build()
    }

    pub(crate) fn get_request(key: String, vbucket_id: u16) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::Get)
            .vbucket_id(vbucket_id)
            .key(key)
            .build()
    }

    pub(crate) fn set_request(key: String, value: impl serde::Serialize, vbucket_id: u16) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::Set)
            .vbucket_id(vbucket_id)
            .extras({
                // empty flags and expiration
                let mut extras = BytesMut::with_capacity(8);
                extras.put_u32(0);
                extras.put_u32(0);
                extras.freeze()
            })
            .key(key)
            .value(Bytes::from(serde_json::to_string(&value).unwrap()))
            .build()
    }

    pub(crate) fn delete_request(key: String, vbucket_id: u16) -> Frame {
        FrameBuilder::request()
            .opcode(Opcode::Delete)
            .vbucket_id(vbucket_id)
            .key(key)
            .build()
    }
}

pub(crate) enum Feature {
    Datatype,
    Tls,
    TcpNodelay,
    MutationSeqno,
    TcpDelay,
    Xattr,
    Xerror,
    SelectBucket,
    Snappy,
    Json,
    Duplex,
}

impl Feature {
    pub(crate) fn as_u16(&self) -> u16 {
        match self {
            Feature::Datatype => 0x0001,
            Feature::Tls => 0x0002,
            Feature::TcpNodelay => 0x0003,
            Feature::MutationSeqno => 0x0004,
            Feature::TcpDelay => 0x0005,
            Feature::Xattr => 0x0006,
            Feature::Xerror => 0x0007,
            Feature::SelectBucket => 0x0008,
            Feature::Snappy => 0x000a,
            Feature::Json => 0x000b,
            Feature::Duplex => 0x000c,
        }
    }
}

impl TryFrom<u16> for Feature {
    type Error = DecodeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(match value {
            0x0001 => Feature::Datatype,
            0x0002 => Feature::Tls,
            0x0003 => Feature::TcpNodelay,
            0x0004 => Feature::MutationSeqno,
            0x0005 => Feature::TcpDelay,
            0x0006 => Feature::Xattr,
            0x0007 => Feature::Xerror,
            0x0008 => Feature::SelectBucket,
            0x000a => Feature::Snappy,
            0x000b => Feature::Json,
            0x000c => Feature::Duplex,
            _ => return Err(anyhow::anyhow!("invalid feature").into()),
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Opcode {
    Hello,
    Get,
    SaslListMech,
    GetClusterConfig,
    SelectBucket,
    SaslAuth,
    Set,
    Delete,
}

impl TryFrom<u8> for Opcode {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0x00 => Opcode::Get,
            0x01 => Opcode::Set,
            0x04 => Opcode::Delete,
            0x1f => Opcode::Hello,
            0x20 => Opcode::SaslListMech,
            0x21 => Opcode::SaslAuth,
            0xb5 => Opcode::GetClusterConfig,
            0x89 => Opcode::SelectBucket,
            _ => return Err(anyhow::anyhow!("invalid opcode").into()),
        })
    }
}

impl From<Opcode> for u8 {
    fn from(opcode: Opcode) -> Self {
        match opcode {
            Opcode::Get => 0x00,
            Opcode::Set => 0x01,
            Opcode::Delete => 0x04,
            Opcode::Hello => 0x1f,
            Opcode::SaslListMech => 0x20,
            Opcode::SaslAuth => 0x21,
            Opcode::GetClusterConfig => 0xb5,
            Opcode::SelectBucket => 0x89,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataType {
    Raw,
    Json,
    SnappyCompressed,
    ExtendedAttributes,
}

impl From<DataType> for u8 {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Raw => 0x00,
            DataType::Json => 0x01,
            DataType::SnappyCompressed => 0x02,
            DataType::ExtendedAttributes => 0x04,
        }
    }
}

impl TryFrom<u8> for DataType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0x00 => DataType::Raw,
            0x01 => DataType::Json,
            0x02 => DataType::SnappyCompressed,
            0x04 => DataType::ExtendedAttributes,
            _ => return Err(anyhow::anyhow!("invalid data_type").into()),
        })
    }
}

const MAX: usize = 8 * 1024 * 1024;
const HEADER_LEN: usize = 24;

impl Decoder for Codec {
    type Item = Frame;

    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 12 {
            // Not enough data to read total body length.
            return Ok(None);
        }

        let mut total_body_length_bytes = [0u8; 4];
        total_body_length_bytes.copy_from_slice(&src[8..12]);
        let total_body_length = u32::from_be_bytes(total_body_length_bytes) as usize;

        if src.len() < HEADER_LEN + total_body_length {
            // The full frame has not yet arrived
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(HEADER_LEN + total_body_length - src.len());

            return Ok(None);
        }

        let magic = Magic::try_from(src.get_u8())?;
        let opcode = Opcode::try_from(src.get_u8())?;
        let key_length = src.get_u16() as usize;
        let extras_length = src.get_u8() as usize;
        let data_type = DataType::try_from(src.get_u8())?;
        let vbucket_id_or_status = src.get_u16();
        let total_body_length = src.get_u32() as usize;
        let opaque = src.get_u32();
        let cas = src.get_u64();
        let extras = src.copy_to_bytes(extras_length);
        let key = src.copy_to_bytes(key_length);
        let value = src.copy_to_bytes(total_body_length - extras_length - key_length);

        return Ok(Some(Frame {
            magic,
            opcode,
            data_type,
            vbucket_id: if magic.is_request() {
                Some(vbucket_id_or_status)
            } else {
                None
            },
            status: if magic.is_response() {
                Some(Status::try_from(vbucket_id_or_status)?)
            } else {
                None
            },
            opaque,
            cas,
            extras,
            key,
            value,
        }));
    }
}

impl Encoder<Frame> for Codec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let total_body_length = frame.key.len() + frame.value.len() + frame.extras.len();
        let len = HEADER_LEN + total_body_length;
        if len > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", len),
            ));
        }

        dst.reserve(len);

        // TODO: Length checks

        dst.put_u8(frame.magic.into());
        dst.put_u8(frame.opcode.into());
        dst.put_u16(frame.key.len() as u16);
        dst.put_u8(frame.extras.len() as u8);
        dst.put_u8(frame.data_type.into());
        // TODO: Check that magic and vbucket/status correspond
        if let Some(vbucket_id) = frame.vbucket_id {
            dst.put_u16(vbucket_id);
        }
        if let Some(status) = frame.status {
            dst.put_u16(status.into());
        }
        dst.put_u32(total_body_length as u32);
        dst.put_u32(frame.opaque);
        dst.put_u64(frame.cas);
        dst.put(frame.extras);
        dst.put(frame.key);
        dst.put(frame.value);

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
