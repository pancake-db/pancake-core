use std::net::IpAddr;

use hyper::{Body, Client as HClient, Method, StatusCode, Request};
use hyper::body::HttpBody;
use hyper::client::HttpConnector;
use protobuf::Message;

use crate::errors::{ClientError, ClientResult};
use uuid::Uuid;

mod api;
mod read;
pub mod types;

#[derive(Clone, Debug)]
pub struct Client {
  pub ip: IpAddr,
  pub port: u16,
  pub h_client: HClient<HttpConnector>,
}

impl Client {
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

pub fn new_correlation_id() -> String {
  Uuid::new_v4().to_string()
}

