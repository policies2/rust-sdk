use http::uri::PathAndQuery;
use prost::Message;
use prost_types::{value::Kind as ProstKind, ListValue, Struct, Value as ProstValue};
use reqwest::{Client as HttpClient, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Once;
use std::time::Duration;
use tonic::client::Grpc;
use tonic::codec::ProstCodec;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_REST_BASE_URL: &str = "https://api.policy2.net/run";
const DEFAULT_RPC_ADDRESS: &str = "shuttle.proxy.rlwy.net:27179";
const POLICY_RPC_PATH: &str = "/policy.v1.PolicyService/RunPolicy";
const FLOW_RPC_PATH: &str = "/policy.v1.FlowService/RunFlow";
static BUGFIXES_PANIC_HOOK: Once = Once::new();

fn default_rest_base_url() -> &'static str {
    DEFAULT_REST_BASE_URL
}

fn default_rpc_address() -> &'static str {
    DEFAULT_RPC_ADDRESS
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    Rest,
    Rpc,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Reference {
    Base,
    #[default]
    Version,
}

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub kind: TransportKind,
    pub base_url: Option<String>,
    pub address: Option<String>,
    pub tls: bool,
}

#[derive(Debug, Clone)]
pub struct ExecutionClientConfig {
    pub api_key: String,
    pub transport: TransportConfig,
    pub timeout: Option<Duration>,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutePolicyRequest {
    #[serde(skip)]
    pub id: String,
    #[serde(skip)]
    pub reference: Reference,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecuteFlowRequest {
    #[serde(skip)]
    pub id: String,
    #[serde(skip)]
    pub reference: Reference,
    pub data: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrchestratorTiming {
    pub go: String,
    pub database: String,
    pub total: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionTiming {
    pub orchestrator: Option<OrchestratorTiming>,
    pub engine: String,
    pub total: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyExecutionResult {
    #[serde(default)]
    pub kind: String,
    pub result: bool,
    pub trace: Value,
    pub rule: Vec<String>,
    pub data: Value,
    #[serde(default)]
    pub error: Value,
    #[serde(default)]
    pub errors: Value,
    pub labels: Value,
    pub execution: Option<ExecutionTiming>,
    pub timings: Option<ExecutionTiming>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowNodeExecution {
    pub database: String,
    pub engine: String,
    pub total: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowNodeResponse {
    #[serde(rename = "nodeId")]
    pub node_id: String,
    #[serde(rename = "nodeType")]
    pub node_type: String,
    pub response: PolicyExecutionData,
    pub execution: Option<FlowNodeExecution>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyExecutionData {
    pub result: bool,
    pub trace: Value,
    pub rule: Vec<String>,
    pub data: Value,
    #[serde(default)]
    pub error: Value,
    #[serde(default)]
    pub errors: Value,
    pub labels: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowExecutionTiming {
    pub orchestrator: String,
    pub database: String,
    pub engine: String,
    pub total: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowExecutionResult {
    #[serde(default)]
    pub kind: String,
    pub result: Value,
    #[serde(rename = "nodeResponse")]
    pub node_response: Vec<FlowNodeResponse>,
    pub execution: Option<FlowExecutionTiming>,
    pub timings: Option<FlowExecutionTiming>,
}

#[derive(Debug)]
pub enum Error {
    Configuration(String),
    Authentication(String),
    Authorization(String),
    Server { message: String, status: u16 },
    Transport(reqwest::Error),
    Decode(String),
    RpcTransport(tonic::transport::Error),
    RpcStatus(Box<tonic::Status>),
    Metadata(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "{message}"),
            Self::Authentication(message) => write!(f, "{message}"),
            Self::Authorization(message) => write!(f, "{message}"),
            Self::Server { message, status } => write!(f, "{message} (status {status})"),
            Self::Transport(err) => write!(f, "REST execution request failed: {err}"),
            Self::Decode(err) => write!(f, "failed to decode response body: {err}"),
            Self::RpcTransport(err) => write!(f, "failed to connect to rpc endpoint: {err}"),
            Self::RpcStatus(status) => write!(f, "RPC execution request failed: {status}"),
            Self::Metadata(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {}

enum Transport {
    Rest {
        client: HttpClient,
        base_url: String,
    },
    Rpc {
        channel: Channel,
    },
}

pub struct ExecutionClient {
    transport: Transport,
    api_key: String,
    user_agent: Option<String>,
}

pub fn init_bugfixes() -> Result<(), reqwest::Error> {
    bugfixes::init_global_from_env()?;
    BUGFIXES_PANIC_HOOK.call_once(bugfixes::install_global_panic_hook);
    Ok(())
}

impl ExecutionClient {
    pub fn new(config: ExecutionClientConfig) -> Result<Self, Error> {
        let _ = init_bugfixes();

        if config.api_key.trim().is_empty() {
            let _ = bugfixes::error!("execution client configuration rejected: missing api_key");
            return Err(Error::Configuration("api_key is required".into()));
        }

        let timeout = config.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let _ = bugfixes::log!(
            "initializing execution client transport={} timeout_ms={} has_user_agent={}",
            match config.transport.kind {
                TransportKind::Rest => "rest",
                TransportKind::Rpc => "rpc",
            },
            timeout.as_millis(),
            config.user_agent.is_some(),
        );

        let transport = match config.transport.kind {
            TransportKind::Rest => {
                let base_url = config
                    .transport
                    .base_url
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(default_rest_base_url());

                let client = HttpClient::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(|err| {
                        let _ =
                            bugfixes::error!("failed to build REST http client: {}", err);
                        Error::Transport(err)
                    })?;

                let _ = bugfixes::info!("configured REST transport base_url={}", base_url);

                Transport::Rest {
                    client,
                    base_url: base_url.trim_end_matches('/').to_string(),
                }
            }
            TransportKind::Rpc => {
                let address = config
                    .transport
                    .address
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(default_rpc_address());

                let uri = normalize_rpc_address(address, config.transport.tls);
                let _ = bugfixes::info!(
                    "configured RPC transport address={} tls={}",
                    uri,
                    config.transport.tls
                );
                let endpoint = Endpoint::from_shared(uri)
                    .map_err(|err| {
                        let _ = bugfixes::error!("invalid rpc address: {}", err);
                        Error::Configuration(format!("invalid rpc address: {err}"))
                    })?
                    .connect_timeout(timeout)
                    .timeout(timeout);

                let endpoint = if config.transport.tls {
                    endpoint
                        .tls_config(ClientTlsConfig::new())
                        .map_err(|err| {
                            let _ = bugfixes::error!(
                                "failed to configure RPC tls transport: {}",
                                err
                            );
                            Error::RpcTransport(err)
                        })?
                } else {
                    endpoint
                };

                Transport::Rpc {
                    channel: endpoint.connect_lazy(),
                }
            }
        };

        Ok(Self {
            transport,
            api_key: config.api_key,
            user_agent: config.user_agent,
        })
    }

    pub async fn execute_policy(
        &self,
        request: ExecutePolicyRequest,
    ) -> Result<PolicyExecutionResult, Error> {
        let reference = request.reference;
        let request_id = request.id.clone();
        let _ = bugfixes::info!(
            "executing policy id={} reference={} transport={}",
            request_id,
            reference_name(reference),
            self.transport_name(),
        );

        match &self.transport {
            Transport::Rest { base_url, client } => {
                let path = policy_path(&request.id, request.reference);
                let mut response: PolicyExecutionResult = self
                    .send_rest(client, base_url, &path, request.data)
                    .await?;
                response.kind = "policy".to_string();
                let _ = bugfixes::info!(
                    "policy execution completed id={} transport=rest result={}",
                    request_id,
                    response.result
                );
                Ok(response)
            }
            Transport::Rpc { channel } => {
                let mut grpc = Grpc::new(channel.clone());
                let rpc_request = if request.reference == Reference::Base {
                    RunPolicyRequest {
                        policy_id: String::new(),
                        data: Some(json_to_struct(request.data)?),
                        base_id: request.id,
                    }
                } else {
                    RunPolicyRequest {
                        policy_id: request.id,
                        data: Some(json_to_struct(request.data)?),
                        base_id: String::new(),
                    }
                };

                let response = self
                    .send_rpc(&mut grpc, POLICY_RPC_PATH, rpc_request)
                    .await?;

                let response = policy_response_from_rpc(response);
                let _ = bugfixes::info!(
                    "policy execution completed id={} transport=rpc result={}",
                    request_id,
                    response.result
                );
                Ok(response)
            }
        }
    }

    pub async fn execute_flow(
        &self,
        request: ExecuteFlowRequest,
    ) -> Result<FlowExecutionResult, Error> {
        let reference = request.reference;
        let request_id = request.id.clone();
        let _ = bugfixes::info!(
            "executing flow id={} reference={} transport={}",
            request_id,
            reference_name(reference),
            self.transport_name(),
        );

        match &self.transport {
            Transport::Rest { base_url, client } => {
                let path = flow_path(&request.id, request.reference);
                let mut response: FlowExecutionResult = self
                    .send_rest(client, base_url, &path, request.data)
                    .await?;
                response.kind = "flow".to_string();
                let _ = bugfixes::info!(
                    "flow execution completed id={} transport=rest nodes={}",
                    request_id,
                    response.node_response.len()
                );
                Ok(response)
            }
            Transport::Rpc { channel } => {
                let mut grpc = Grpc::new(channel.clone());
                let rpc_request = if request.reference == Reference::Base {
                    RunFlowRequest {
                        flow_id: String::new(),
                        data: Some(json_to_struct(request.data)?),
                        base_id: request.id,
                    }
                } else {
                    RunFlowRequest {
                        flow_id: request.id,
                        data: Some(json_to_struct(request.data)?),
                        base_id: String::new(),
                    }
                };

                let response = self.send_rpc(&mut grpc, FLOW_RPC_PATH, rpc_request).await?;
                let response = flow_response_from_rpc(response);
                let _ = bugfixes::info!(
                    "flow execution completed id={} transport=rpc nodes={}",
                    request_id,
                    response.node_response.len()
                );
                Ok(response)
            }
        }
    }

    async fn send_rest<T>(
        &self,
        client: &HttpClient,
        base_url: &str,
        path: &str,
        data: Value,
    ) -> Result<T, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let url = format!("{base_url}{path}");
        let _ = bugfixes::log!("sending REST request {}", url);
        let mut builder = client
            .post(&url)
            .header("content-type", "application/json")
            .header("x-api-key", &self.api_key)
            .json(&json!({ "data": data }));

        if let Some(user_agent) = &self.user_agent {
            builder = builder.header("user-agent", user_agent);
        }

        let response = builder.send().await.map_err(|err| {
            let _ = bugfixes::error!("REST request failed url={} error={}", url, err);
            Error::Transport(err)
        })?;
        let status = response.status();
        let _ = bugfixes::log!("received REST response url={} status={}", url, status);
        let body = response.text().await.map_err(|err| {
            let _ = bugfixes::error!(
                "failed to read REST response body url={} status={} error={}",
                url,
                status,
                err
            );
            Error::Transport(err)
        })?;

        decode_rest_response(status, &body)
    }

    async fn send_rpc<TRequest, TResponse>(
        &self,
        grpc: &mut Grpc<Channel>,
        path: &'static str,
        request: TRequest,
    ) -> Result<TResponse, Error>
    where
        TRequest: Message + Send + Sync + 'static,
        TResponse: Message + Default + Send + Sync + 'static,
    {
        let mut tonic_request = Request::new(request);
        let api_key = MetadataValue::try_from(self.api_key.as_str())
            .map_err(|err| {
                let _ = bugfixes::error!("invalid api_key metadata: {}", err);
                Error::Metadata(format!("invalid api_key metadata: {err}"))
            })?;
        tonic_request.metadata_mut().insert("x-api-key", api_key);

        let path = PathAndQuery::from_static(path);
        let codec = ProstCodec::default();
        let _ = bugfixes::log!("sending RPC request path={}", path.as_str());

        grpc.unary(tonic_request, path, codec)
            .await
            .map(|response| {
                let _ = bugfixes::log!("received RPC response");
                response.into_inner()
            })
            .map_err(map_rpc_status)
    }

    fn transport_name(&self) -> &'static str {
        match self.transport {
            Transport::Rest { .. } => "rest",
            Transport::Rpc { .. } => "rpc",
        }
    }
}

fn policy_path(id: &str, reference: Reference) -> String {
    match reference {
        Reference::Base => format!("/policy/{id}"),
        Reference::Version => format!("/policy_version/{id}"),
    }
}

fn flow_path(id: &str, reference: Reference) -> String {
    match reference {
        Reference::Base => format!("/flow/{id}"),
        Reference::Version => format!("/flow_version/{id}"),
    }
}

fn normalize_rpc_address(address: &str, tls: bool) -> String {
    if address.contains("://") {
        let _ = bugfixes::log!("using explicit RPC address {}", address);
        return address.to_string();
    }
    if tls {
        format!("https://{address}")
    } else {
        format!("http://{address}")
    }
}

fn map_http_error(status: StatusCode, body: String) -> Error {
    let message = if body.trim().is_empty() {
        format!("request failed with status {status}")
    } else {
        body
    };

    let _ = bugfixes::error!("REST request returned status={} body={}", status, message);

    match status {
        StatusCode::UNAUTHORIZED => Error::Authentication(message),
        StatusCode::FORBIDDEN => Error::Authorization(message),
        _ => Error::Server {
            message,
            status: status.as_u16(),
        },
    }
}

fn decode_rest_response<T>(status: StatusCode, body: &str) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    if !status.is_success() {
        return Err(map_http_error(status, body.to_string()));
    }

    serde_json::from_str::<T>(body).map_err(|err| {
        let _ = bugfixes::error!(
            "failed to decode REST response status={} error={} body={}",
            status,
            err,
            body
        );
        Error::Decode(err.to_string())
    })
}

fn map_rpc_status(status: tonic::Status) -> Error {
    let _ = bugfixes::error!(
        "RPC request failed code={} message={}",
        status.code(),
        status.message()
    );
    match status.code() {
        Code::Unauthenticated => Error::Authentication(status.message().to_string()),
        Code::PermissionDenied => Error::Authorization(status.message().to_string()),
        _ => Error::RpcStatus(Box::new(status)),
    }
}

fn json_to_struct(value: Value) -> Result<Struct, Error> {
    match value {
        Value::Object(map) => {
            let mut fields = BTreeMap::new();
            for (key, value) in map {
                fields.insert(key, json_to_prost_value(value)?);
            }
            Ok(Struct { fields })
        }
        _ => {
            let _ = bugfixes::error!("request data must be a JSON object");
            Err(Error::Configuration(
                "request data must be a JSON object".to_string(),
            ))
        }
    }
}

fn json_to_prost_value(value: Value) -> Result<ProstValue, Error> {
    let kind = match value {
        Value::Null => ProstKind::NullValue(0),
        Value::Bool(value) => ProstKind::BoolValue(value),
        Value::Number(value) => ProstKind::NumberValue(
            value
                .as_f64()
                .ok_or_else(|| {
                    let _ = bugfixes::error!("invalid numeric value in request payload");
                    Error::Configuration("invalid numeric value".into())
                })?,
        ),
        Value::String(value) => ProstKind::StringValue(value),
        Value::Array(values) => ProstKind::ListValue(ListValue {
            values: values
                .into_iter()
                .map(json_to_prost_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Value::Object(map) => {
            let mut fields = BTreeMap::new();
            for (key, value) in map {
                fields.insert(key, json_to_prost_value(value)?);
            }
            ProstKind::StructValue(Struct { fields })
        }
    };

    Ok(ProstValue { kind: Some(kind) })
}

fn prost_value_to_json(value: ProstValue) -> Value {
    match value.kind {
        Some(ProstKind::NullValue(_)) | None => Value::Null,
        Some(ProstKind::BoolValue(value)) => Value::Bool(value),
        Some(ProstKind::NumberValue(value)) => {
            Value::Number(Number::from_f64(value).unwrap_or_else(|| Number::from(0)))
        }
        Some(ProstKind::StringValue(value)) => Value::String(value),
        Some(ProstKind::StructValue(value)) => prost_struct_to_json(value),
        Some(ProstKind::ListValue(value)) => {
            Value::Array(value.values.into_iter().map(prost_value_to_json).collect())
        }
    }
}

fn prost_struct_to_json(value: Struct) -> Value {
    let mut map = Map::new();
    for (key, value) in value.fields {
        map.insert(key, prost_value_to_json(value));
    }
    Value::Object(map)
}

fn optional_struct_to_json(value: Option<Struct>) -> Value {
    value.map(prost_struct_to_json).unwrap_or(Value::Null)
}

fn optional_value_to_json(value: Option<ProstValue>) -> Value {
    value.map(prost_value_to_json).unwrap_or(Value::Null)
}

fn reference_name(reference: Reference) -> &'static str {
    match reference {
        Reference::Base => "base",
        Reference::Version => "version",
    }
}

fn policy_response_from_rpc(response: RunResponse) -> PolicyExecutionResult {
    PolicyExecutionResult {
        kind: "policy".into(),
        result: response.result,
        trace: optional_struct_to_json(response.trace),
        rule: response.rule,
        data: optional_struct_to_json(response.data),
        error: optional_struct_to_json(response.error),
        errors: Value::Null,
        labels: optional_struct_to_json(response.labels),
        execution: response.execution.map(|execution| ExecutionTiming {
            orchestrator: execution
                .orchestrator
                .map(|orchestrator| OrchestratorTiming {
                    go: orchestrator.go,
                    database: orchestrator.database,
                    total: orchestrator.total,
                }),
            engine: execution.engine,
            total: execution.total,
        }),
        timings: None,
    }
}

fn flow_response_from_rpc(response: FlowResponse) -> FlowExecutionResult {
    FlowExecutionResult {
        kind: "flow".into(),
        result: optional_value_to_json(response.result),
        node_response: response
            .node_response
            .into_iter()
            .map(|node| FlowNodeResponse {
                node_id: node.node_id,
                node_type: node.node_type,
                response: {
                    let run_response = node.response.unwrap_or_default();
                    PolicyExecutionData {
                        result: run_response.result,
                        trace: optional_struct_to_json(run_response.trace),
                        rule: run_response.rule,
                        data: optional_struct_to_json(run_response.data),
                        error: optional_struct_to_json(run_response.error),
                        errors: Value::Null,
                        labels: optional_struct_to_json(run_response.labels),
                    }
                },
                execution: node.execution.map(|execution| FlowNodeExecution {
                    database: execution.database,
                    engine: execution.engine,
                    total: execution.total,
                }),
            })
            .collect(),
        execution: response.execution.map(|execution| FlowExecutionTiming {
            orchestrator: execution.orchestrator,
            database: execution.database,
            engine: execution.engine,
            total: execution.total,
        }),
        timings: None,
    }
}

#[derive(Clone, PartialEq, Message)]
struct RunPolicyRequest {
    #[prost(string, tag = "1")]
    policy_id: String,
    #[prost(message, optional, tag = "2")]
    data: Option<Struct>,
    #[prost(string, tag = "3")]
    base_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct RunFlowRequest {
    #[prost(string, tag = "1")]
    flow_id: String,
    #[prost(message, optional, tag = "2")]
    data: Option<Struct>,
    #[prost(string, tag = "3")]
    base_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct RunResponse {
    #[prost(bool, tag = "1")]
    result: bool,
    #[prost(message, optional, tag = "2")]
    trace: Option<Struct>,
    #[prost(string, repeated, tag = "3")]
    rule: Vec<String>,
    #[prost(message, optional, tag = "4")]
    data: Option<Struct>,
    #[prost(message, optional, tag = "5")]
    error: Option<Struct>,
    #[prost(message, optional, tag = "6")]
    labels: Option<Struct>,
    #[prost(message, optional, tag = "7")]
    execution: Option<RpcExecutionTiming>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcExecutionTiming {
    #[prost(message, optional, tag = "1")]
    orchestrator: Option<RpcOrchestratorTiming>,
    #[prost(string, tag = "2")]
    engine: String,
    #[prost(string, tag = "3")]
    total: String,
}

#[derive(Clone, PartialEq, Message)]
struct RpcOrchestratorTiming {
    #[prost(string, tag = "1")]
    go: String,
    #[prost(string, tag = "2")]
    database: String,
    #[prost(string, tag = "3")]
    total: String,
}

#[derive(Clone, PartialEq, Message)]
struct FlowResponse {
    #[prost(message, optional, tag = "1")]
    result: Option<ProstValue>,
    #[prost(message, repeated, tag = "2")]
    node_response: Vec<RpcFlowNodeResponse>,
    #[prost(message, optional, tag = "3")]
    execution: Option<RpcFlowExecutionTiming>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcFlowNodeResponse {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(string, tag = "2")]
    node_type: String,
    #[prost(message, optional, tag = "3")]
    response: Option<RunResponse>,
    #[prost(message, optional, tag = "4")]
    execution: Option<RpcFlowNodeExecution>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcFlowNodeExecution {
    #[prost(string, tag = "1")]
    database: String,
    #[prost(string, tag = "2")]
    engine: String,
    #[prost(string, tag = "3")]
    total: String,
}

#[derive(Clone, PartialEq, Message)]
struct RpcFlowExecutionTiming {
    #[prost(string, tag = "1")]
    orchestrator: String,
    #[prost(string, tag = "2")]
    database: String,
    #[prost(string, tag = "3")]
    engine: String,
    #[prost(string, tag = "4")]
    total: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_api_key() {
        let result = ExecutionClient::new(ExecutionClientConfig {
            api_key: String::new(),
            transport: TransportConfig {
                kind: TransportKind::Rest,
                base_url: Some("https://api.policy2.net".into()),
                address: None,
                tls: false,
            },
            timeout: None,
            user_agent: None,
        });

        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn defaults_rest_base_url() {
        let client = ExecutionClient::new(ExecutionClientConfig {
            api_key: "pk_test".into(),
            transport: TransportConfig {
                kind: TransportKind::Rest,
                base_url: None,
                address: None,
                tls: false,
            },
            timeout: None,
            user_agent: None,
        })
        .expect("rest client should default base url");

        match client.transport {
            Transport::Rest { base_url, .. } => assert_eq!(default_rest_base_url(), base_url),
            Transport::Rpc { .. } => panic!("expected rest transport"),
        }
    }

    #[test]
    fn defaults_rpc_address() {
        assert_eq!(default_rpc_address(), "shuttle.proxy.rlwy.net:27179");
        assert_eq!(
            normalize_rpc_address(default_rpc_address(), false),
            "http://shuttle.proxy.rlwy.net:27179"
        );
    }

    #[test]
    fn path_helpers_work() {
        assert_eq!(
            "/policy/base-123",
            policy_path("base-123", Reference::Base)
        );
        assert_eq!(
            "/flow_version/flow-123",
            flow_path("flow-123", Reference::Version)
        );
    }

    #[test]
    fn normalize_rpc_address_adds_scheme() {
        assert_eq!(
            "http://127.0.0.1:8081",
            normalize_rpc_address("127.0.0.1:8081", false)
        );
        assert_eq!(
            "https://127.0.0.1:8081",
            normalize_rpc_address("127.0.0.1:8081", true)
        );
        assert_eq!(
            "https://example.com",
            normalize_rpc_address("https://example.com", false)
        );
    }

    #[test]
    fn maps_rpc_status_codes() {
        let unauthenticated = map_rpc_status(tonic::Status::unauthenticated("bad key"));
        assert!(matches!(unauthenticated, Error::Authentication(_)));

        let forbidden = map_rpc_status(tonic::Status::permission_denied("forbidden"));
        assert!(matches!(forbidden, Error::Authorization(_)));

        let internal = map_rpc_status(tonic::Status::internal("boom"));
        assert!(matches!(internal, Error::RpcStatus(_)));
    }

    #[test]
    fn json_struct_round_trip_works() {
        let value = json!({
            "name": "Bob",
            "age": 25,
            "active": true,
            "scores": [1, 2, 3],
            "meta": {
                "country": "GB"
            }
        });

        let converted = json_to_struct(value.clone()).unwrap();
        let round_trip = prost_struct_to_json(converted);

        assert_eq!(value["name"], round_trip["name"]);
        assert_eq!(value["meta"], round_trip["meta"]);
        assert_eq!(value["active"], round_trip["active"]);
        assert_eq!(25.0, round_trip["age"].as_f64().unwrap());
        assert_eq!(3, round_trip["scores"].as_array().unwrap().len());
    }

    #[test]
    fn policy_response_from_rpc_maps_fields() {
        let response = RunResponse {
            result: true,
            trace: Some(Struct {
                fields: BTreeMap::from([(
                    "step".into(),
                    ProstValue {
                        kind: Some(ProstKind::StringValue("ok".into())),
                    },
                )]),
            }),
            rule: vec!["rule".into()],
            data: Some(Struct {
                fields: BTreeMap::from([(
                    "approved".into(),
                    ProstValue {
                        kind: Some(ProstKind::BoolValue(true)),
                    },
                )]),
            }),
            error: None,
            labels: None,
            execution: Some(RpcExecutionTiming {
                orchestrator: Some(RpcOrchestratorTiming {
                    go: "1".into(),
                    database: "2".into(),
                    total: "3".into(),
                }),
                engine: "4".into(),
                total: "5".into(),
            }),
        };

        let parsed = policy_response_from_rpc(response);
        assert_eq!("policy", parsed.kind);
        assert!(parsed.result);
        assert_eq!(
            Some("4"),
            parsed.execution.as_ref().map(|value| value.engine.as_str())
        );
    }

    #[test]
    fn flow_response_from_rpc_maps_fields() {
        let response = FlowResponse {
            result: Some(ProstValue {
                kind: Some(ProstKind::StringValue("approved".into())),
            }),
            node_response: vec![RpcFlowNodeResponse {
                node_id: "node-1".into(),
                node_type: "policy".into(),
                response: Some(RunResponse {
                    result: true,
                    trace: None,
                    rule: vec!["rule".into()],
                    data: None,
                    error: None,
                    labels: None,
                    execution: None,
                }),
                execution: Some(RpcFlowNodeExecution {
                    database: "1".into(),
                    engine: "2".into(),
                    total: "3".into(),
                }),
            }],
            execution: Some(RpcFlowExecutionTiming {
                orchestrator: "1".into(),
                database: "2".into(),
                engine: "3".into(),
                total: "4".into(),
            }),
        };

        let parsed = flow_response_from_rpc(response);
        assert_eq!("flow", parsed.kind);
        assert_eq!(1, parsed.node_response.len());
        assert_eq!("node-1", parsed.node_response[0].node_id);
        assert_eq!(
            Some("3"),
            parsed.execution.as_ref().map(|value| value.engine.as_str())
        );
    }

    #[test]
    fn decodes_rest_policy_response() {
        let result: PolicyExecutionResult = decode_rest_response(
            StatusCode::OK,
            r#"{"kind":"","result":true,"trace":null,"rule":["rule"],"data":{"approved":true},"error":null,"labels":null}"#,
        )
        .unwrap();

        assert!(result.result);
        assert_eq!(vec!["rule".to_string()], result.rule);
    }

    #[test]
    fn decodes_rest_flow_response() {
        let result: FlowExecutionResult = decode_rest_response(
            StatusCode::OK,
            r#"{"kind":"","result":{"approved":true},"nodeResponse":[{"nodeId":"node-1","nodeType":"policy","response":{"result":true,"trace":null,"rule":["rule"],"data":{"approved":true},"error":null,"labels":null}}]}"#,
        )
        .unwrap();

        assert_eq!(1, result.node_response.len());
        assert_eq!("node-1", result.node_response[0].node_id);
    }

    #[test]
    fn decode_rest_response_maps_statuses() {
        assert!(matches!(
            decode_rest_response::<PolicyExecutionResult>(StatusCode::UNAUTHORIZED, "bad key"),
            Err(Error::Authentication(_))
        ));
        assert!(matches!(
            decode_rest_response::<PolicyExecutionResult>(StatusCode::FORBIDDEN, "forbidden"),
            Err(Error::Authorization(_))
        ));
        assert!(matches!(
            decode_rest_response::<PolicyExecutionResult>(
                StatusCode::INTERNAL_SERVER_ERROR,
                "boom"
            ),
            Err(Error::Server { .. })
        ));
    }

    #[test]
    fn decode_rest_response_rejects_invalid_json() {
        assert!(matches!(
            decode_rest_response::<PolicyExecutionResult>(StatusCode::OK, "{"),
            Err(Error::Decode(_))
        ));
    }
}
