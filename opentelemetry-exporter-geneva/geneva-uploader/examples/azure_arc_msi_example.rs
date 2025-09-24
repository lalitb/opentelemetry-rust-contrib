// Azure Arc MSI Authentication Example for Geneva Config Service
//
// This example demonstrates how to use Azure Arc Managed Identity
// authentication with the Geneva config service instead of certificates.

use geneva_uploader::{
    AzureArcManagedIdentityCredential, AuthMethod, GenevaConfigClient, GenevaConfigClientConfig,
};
use azure_core::{credentials::TokenCredential, http::new_http_client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Azure Arc MSI Authentication Example");
    println!("====================================");

    // Example 1: Direct usage of Azure Arc MSI credential
    println!("\n1. Testing Azure Arc MSI credential directly...");

    let http_client = new_http_client();
    let credential = AzureArcManagedIdentityCredential::new(http_client)?;

    match credential.get_token(&["https://management.azure.com/.default"], None).await {
        Ok(token) => {
            println!("✓ Successfully obtained Azure Arc MSI token");
            println!("  Token length: {} characters", token.token.secret().len());
            println!("  Expires at: {:?}", token.expires_on);
        }
        Err(e) => {
            println!("✗ Failed to get Azure Arc MSI token: {}", e);
            println!("  Make sure you're running on an Azure Arc-enabled machine");
            println!("  and have access to the Arc agent's token files.");
        }
    }

    // Example 2: Using Azure Arc MSI with Geneva Config Client
    println!("\n2. Testing Geneva Config Client with Azure Arc MSI...");

    let config = GenevaConfigClientConfig {
        endpoint: std::env::var("GENEVA_ENDPOINT")
            .unwrap_or_else(|_| "https://your-geneva-config-endpoint.com".to_string()),
        environment: std::env::var("GENEVA_ENVIRONMENT")
            .unwrap_or_else(|_| "prod".to_string()),
        account: std::env::var("GENEVA_ACCOUNT")
            .unwrap_or_else(|_| "your-account".to_string()),
        namespace: std::env::var("GENEVA_NAMESPACE")
            .unwrap_or_else(|_| "your-namespace".to_string()),
        region: std::env::var("GENEVA_REGION")
            .unwrap_or_else(|_| "westus2".to_string()),
        config_major_version: std::env::var("GENEVA_CONFIG_MAJOR_VERSION")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        auth_method: AuthMethod::AzureArcManagedIdentity,
    };

    match GenevaConfigClient::new(config) {
        Ok(client) => {
            println!("✓ Successfully created Geneva Config Client with Azure Arc MSI");

            match client.get_ingestion_info().await {
                Ok((ingestion_info, moniker_info, endpoint)) => {
                    println!("✓ Successfully retrieved Geneva ingestion info");
                    println!("  Ingestion endpoint: {}", ingestion_info.endpoint);
                    println!("  Token endpoint: {}", endpoint);
                    println!("  Moniker name: {}", moniker_info.name);
                    println!("  Account group: {}", moniker_info.account_group);
                }
                Err(e) => {
                    println!("✗ Failed to get ingestion info: {}", e);
                    println!("  Check your Geneva configuration and network connectivity");
                }
            }
        }
        Err(e) => {
            println!("✗ Failed to create Geneva Config Client: {}", e);
        }
    }

    println!("\n3. Environment setup information:");
    println!("   To use Azure Arc MSI authentication, ensure:");
    println!("   • You're running on an Azure Arc-enabled machine");
    println!("   • The Azure Connected Machine Agent is installed and running");
    println!("   • Your machine has the required permissions for Geneva access");
    println!("   • IDENTITY_ENDPOINT environment variable is set (if not using default)");

    println!("\n4. Configuration environment variables:");
    println!("   Set these variables to customize the Geneva config:");
    println!("   • GENEVA_ENDPOINT: Geneva Config Service endpoint URL");
    println!("   • GENEVA_ENVIRONMENT: Environment name (e.g., 'prod', 'dev')");
    println!("   • GENEVA_ACCOUNT: Your Geneva account name");
    println!("   • GENEVA_NAMESPACE: Your service namespace");
    println!("   • GENEVA_REGION: Azure region (e.g., 'westus2')");
    println!("   • GENEVA_CONFIG_MAJOR_VERSION: Configuration schema version");

    Ok(())
}