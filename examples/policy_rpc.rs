use policies::{
    init_bugfixes, ExecutePolicyRequest, ExecutionClient, ExecutionClientConfig, Reference,
    TransportConfig, TransportKind,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_bugfixes()?;

    let client = ExecutionClient::new(ExecutionClientConfig {
        api_key: std::env::var("POLICY_API_KEY").unwrap_or_else(|_| "pk_live_example".into()),
        transport: TransportConfig {
            kind: TransportKind::Rpc,
            base_url: None,
            // gRPC has no hosted endpoint yet; this targets a locally running API.
            // Plaintext is only allowed to loopback addresses; use tls: true otherwise.
            address: Some(
                std::env::var("POLICY_RPC_ADDRESS").unwrap_or_else(|_| "localhost:8082".into()),
            ),
            tls: false,
        },
        timeout: None,
        user_agent: None,
    })?;

    let response = client
        .execute_policy(ExecutePolicyRequest {
            id: "3b7d4b2a-9aa0-4b6d-a1b4-9dcf11ce12ab".into(),
            reference: Reference::Base,
            data: json!({
                "drivingTest": {
                    "person": {
                        "name": "Bob",
                        "dateOfBirth": "1990-01-01"
                    }
                }
            }),
        })
        .await?;

    println!("Policy result: {}", response.result);
    println!("Execution timing: {:?}", response.execution);
    Ok(())
}
