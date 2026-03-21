# `policies2-rust-sdk`

Execute stored policies and flows over REST or gRPC using API keys only.

This SDK mirrors the narrow scope of the TypeScript package:

- execute policies
- execute flows
- authenticate with `x-api-key`
- choose REST or gRPC transport

It does not support creating, updating, publishing, or administering resources.

## Install

```toml
[dependencies]
policies2-rust-sdk = "0.1"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Usage

```rust
use policies2_rust_sdk::{
    ExecutePolicyRequest, ExecutionClient, ExecutionClientConfig, Reference, TransportConfig,
    TransportKind,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ExecutionClient::new(ExecutionClientConfig {
        api_key: std::env::var("POLICY_API_KEY")?,
        transport: TransportConfig {
            kind: TransportKind::Rest,
            base_url: Some("https://api.policy2.net".into()),
            address: None,
            tls: false,
        },
        timeout: None,
        user_agent: None,
    })?;

    let result = client
        .execute_policy(ExecutePolicyRequest {
            id: "3b7d4b2a-9aa0-4b6d-a1b4-9dcf11ce12ab".into(),
            reference: Reference::Base,
            data: json!({
                "user": { "age": 25 }
            }),
        })
        .await?;

    println!("{}", result.result);
    Ok(())
}
```

## Examples

- REST policy execution: [`examples/policy_rest.rs`](./examples/policy_rest.rs)
- REST flow execution: [`examples/flow_rest.rs`](./examples/flow_rest.rs)
- RPC policy execution: [`examples/policy_rpc.rs`](./examples/policy_rpc.rs)
- RPC flow execution: [`examples/flow_rpc.rs`](./examples/flow_rpc.rs)
