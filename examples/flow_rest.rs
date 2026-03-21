use policies2_rust_sdk::{
    ExecuteFlowRequest, ExecutionClient, ExecutionClientConfig, Reference, TransportConfig,
    TransportKind,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ExecutionClient::new(ExecutionClientConfig {
        api_key: std::env::var("POLICY_API_KEY").unwrap_or_else(|_| "pk_live_example".into()),
        transport: TransportConfig {
            kind: TransportKind::Rest,
            base_url: Some(
                std::env::var("POLICY_API_URL")
                    .unwrap_or_else(|_| "https://api.policy2.net".into()),
            ),
            address: None,
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
                    },
                    "scores": {
                        "theory": {
                            "multipleChoice": 47,
                            "hazardPerception": 70
                        },
                        "practical": {
                            "major": false,
                            "minor": 6
                        }
                    }
                }
            }),
        })
        .await?;

    println!("Flow result: {}", response.result);
    println!("Visited nodes: {}", response.node_response.len());
    Ok(())
}
