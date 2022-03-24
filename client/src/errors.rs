use std::fmt;
use std::fmt::{Display, Formatter};
use std::string::FromUtf8Error;

use tonic::{Code, Status};

trait OtherUpcastable: std::error::Error {}
impl OtherUpcastable for FromUtf8Error {}
#[cfg(feature = "read")]
impl OtherUpcastable for pancake_db_core::errors::CoreError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientError {
  pub message: String,
  pub kind: ClientErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientErrorKind {
  Connection,
  Grpc {
    code: Code,
  },
  Other,
}

impl Display for ClientErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let s = match &self {
      ClientErrorKind::Connection => "connection error".to_string(),
      ClientErrorKind::Grpc { code } => format!("GRPC error {}", code),
      ClientErrorKind::Other => "client-side error".to_string(),
    };
    f.write_str(&s)
  }
}

impl ClientError {
  pub fn other(message: String) -> Self {
    ClientError {
      message,
      kind: ClientErrorKind::Other,
    }
  }
}

impl Display for ClientError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "PancakeDB {}, message: {}",
      self.kind,
      self.message,
    )
  }
}

impl<T> From<T> for ClientError where T: OtherUpcastable {
  fn from(e: T) -> ClientError {
    ClientError {
      message: e.to_string(),
      kind: ClientErrorKind::Other,
    }
  }
}

impl From<tonic::transport::Error> for ClientError {
  fn from(err: tonic::transport::Error) -> Self {
    ClientError {
      message: err.to_string(),
      kind: ClientErrorKind::Connection,
    }
  }
}

impl From<Status> for ClientError {
  fn from(status: Status) -> Self {
    ClientError {
      message: status.message().to_string(),
      kind: ClientErrorKind::Grpc { code: status.code(), },
    }
  }
}

impl std::error::Error for ClientError {}

pub type ClientResult<T> = Result<T, ClientError>;
