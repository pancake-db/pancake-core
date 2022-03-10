use std::net::IpAddr;

use hyper::{Body, Client as HClient, Method, Request, StatusCode};
use hyper::body::HttpBody;
use hyper::client::HttpConnector;
use protobuf::Message;

use crate::errors::{ClientError, ClientResult};

mod api;
mod read;

/// The best way to communicate with a PancakeDB server from Rust.
///
/// Supports the PancakeDB API.
/// Additionally, since PancakeDB reads return raw byte data in a compressed
/// format, `Client` supports some higher-level functionality for reading
/// whole segments into a meaningful representation.
#[derive(Clone, Debug)]
pub struct Client {
  ip: IpAddr,
  port: u16,
  h_client: HClient<HttpConnector>,
}

impl Client {
  /// Creates a new client, given an IP and port.
  pub fn from_ip_port(ip: IpAddr, port: u16) -> Self {
    let h_client = HClient::new();
    Client {
      ip,
      port,
      h_client,
    }
  }

  fn rest_endpoint(&self, name: &str) -> String {
    format!("http://{}:{}/rest/{}", self.ip, self.port, name)
  }

  async fn simple_json_request_bytes<Req: Message>(
    &self,
    endpoint_name: &str,
    method: Method,
    req: &Req
  ) -> ClientResult<Vec<u8>> {
    let uri = self.rest_endpoint(endpoint_name);
    let pb_str = protobuf::json::print_to_string(req)?;

    let http_req = Request::builder()
      .method(method)
      .uri(&uri)
      .header("Content-Type", "application/json")
      .body(Body::from(pb_str))?;
    let mut resp = self.h_client.request(http_req).await?;
    let status = resp.status();
    let mut content = Vec::new();
    while let Some(chunk) = resp.body_mut().data().await {
      content.extend(chunk?.to_vec());
    }

    if status != StatusCode::OK {
      return Err(ClientError::http(status, content));
    }
    Ok(content)
  }

  async fn simple_json_request<Req: Message, Resp: Message>(
    &self,
    endpoint_name: &str,
    method: Method,
    req: &Req
  ) -> ClientResult<Resp> {
    let bytes = self.simple_json_request_bytes(
      endpoint_name,
      method,
      req
    ).await?;

    let content = String::from_utf8(bytes)?;
    let mut res = Resp::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }
}
