use policies::{
    ExecuteFlowRequest, ExecutionClient, ExecutionClientConfig, Reference, TransportConfig,
    TransportKind,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ExecutionClient::new(ExecutionClientConfig {
        api_key: std::env::var("POLICY_API_KEY").unwrap_or_else(|_| "pk_live_example".into()),
        transport: TransportConfig {
            kind: TransportKind::Rpc,
            base_url: None,
            address: Some(
                std::env::var("POLICY_RPC_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8081".into()),
            ),
            tls: false,
        },
        timeout: None,
        user_agent: None,
    })?;

    let response = client
        .execute_flow(ExecuteFlowRequest {
            id: "ae6fb044-ad2b-45fd-82d1-0d2f1fa176a5".into(),
            reference: Reference::Base,
            data: json!({
                "drivingTest": {
                    "person": {
                        "name": "Alice",
                        "dateOfBirth": "1992-05-12"
                    }
                }
            }),
        })
        .await?;

    println!("Flow result: {}", response.result);
    println!("Visited nodes: {}", response.node_response.len());
    Ok(())
}
