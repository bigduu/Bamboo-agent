use reqwest::Client;
use serde::Deserialize;

const GITHUB_CLIENT_ID: &str = "Iv1.b507a08c87ecfe98";
const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";

/// Device code response from GitHub
#[derive(Debug, Deserialize)]
pub struct DeviceCodeResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(rename = "expires_in")]
    pub expires_in: u64,
    pub interval: u64,
}

/// Get device code from GitHub
pub async fn get_device_code(client: &Client) -> Result<DeviceCodeResponse, String> {
    let params = [("client_id", GITHUB_CLIENT_ID), ("scope", "read:user")];

    let response = client
        .post(DEVICE_CODE_URL)
        .header("Accept", "application/json")
        .header("User-Agent", "BambooCopilot/1.0")
        .form(&params)
        .send()
        .await
        .map_err(|e| format!("Failed to request device code: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(format!(
            "Device code request failed: HTTP {} - {}",
            status, text
        ));
    }

    let device_code: DeviceCodeResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse device code response: {}", e))?;

    Ok(device_code)
}

/// Present device code to user
pub fn present_device_code(device_code: &DeviceCodeResponse) {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║     🔐 GitHub Copilot Authorization Required              ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("  1. Open your browser and navigate to:");
    println!("     {}", device_code.verification_uri);
    println!();
    println!("  2. Enter the following code:");
    println!();
    println!("     ┌─────────────────────────┐");
    println!("     │  {:^23} │", device_code.user_code);
    println!("     └─────────────────────────┘");
    println!();
    println!("  3. Click 'Authorize' and wait...");
    println!();
    println!(
        "  ⏳ Waiting for authorization (expires in {} seconds)...",
        device_code.expires_in
    );
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_github_client_id() {
        assert_eq!(GITHUB_CLIENT_ID, "Iv1.b507a08c87ecfe98");
    }
}
