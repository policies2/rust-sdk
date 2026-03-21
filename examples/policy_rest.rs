use policies2_rust_sdk::{
    ExecutePolicyRequest, ExecutionClient, ExecutionClientConfig, Reference, TransportConfig,
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
        .execute_policy(ExecutePolicyRequest {
            id: "3b7d4b2a-9aa0-4b6d-a1b4-9dcf11ce12ab".into(),
            reference: Reference::Base,
            data: json!({
                "drivingTest": {
                    "person": {
                        "name": "Bob",
                        "dateOfBirth": "1990-01-01"
                    },
                    "scores": {
                        "theory": {
                            "multipleChoice": 45,
                            "hazardPerception": 75
                        },
                        "practical": {
                            "major": false,
                            "minor": 13
                        }
                    }
                }
            }),
        })
        .await?;

    println!("Policy result: {}", response.result);
    Ok(())
}
