use reqwest::{
    header::{HeaderMap, HeaderValue},
    Response,
};
use serde_json::json;

pub struct GrafanaRender {
    pub user_id: String,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
}

pub struct DiscordWebhookMessage {
    pub content: String,
    pub graph: Option<GrafanaRender>,
}

fn get_grafana_query_details(details: GrafanaRender) -> String {
    format!(
        "&var-targetUser={}&from={}000&to={}000",
        details.user_id, details.start_timestamp, details.end_timestamp
    )
}

pub async fn send_discord_webhook(
    http_client: reqwest::Client,
    grafana_uri_base: &str,
    grafana_api_token: &str,
    webhook_url: &str,
    message: DiscordWebhookMessage,
) -> Result<(), reqwest::Error> {
    let response: Response;
    if let Some(details) = message.graph {
        let image = http_client
            .get(format!(
                "{}{}",
                grafana_uri_base,
                get_grafana_query_details(details)
            ))
            .header("Authorization", grafana_api_token)
            .send()
            .await?
            .bytes()
            .await?;

        let message_data = json!({
            "embeds": [
                {
                    "title": "BotBlock Detection",
                    "description": message.content,
                    "image": {
                        "url": "attachment://graph.png"
                    }
                }
            ]
        });

        let mut headers = HeaderMap::new();
        headers.append("Content-Type", HeaderValue::from_static("image/png"));

        let form = reqwest::multipart::Form::new()
            .text("payload_json", message_data.to_string())
            .part(
                "files[0]",
                reqwest::multipart::Part::bytes(image.to_vec())
                    .file_name("graph.png")
                    .headers(headers),
            );

        response = http_client.post(webhook_url).multipart(form).send().await?;
    } else {
        let message_data = json!({
            "embeds": [
                {
                    "title": "BotBlock Detection",
                    "description": message.content,
                }
            ]
        });

        response = http_client
            .post(webhook_url)
            .json(&message_data)
            .send()
            .await?;
    }

    if !response.status().is_success() {
        log::error!("Failed to send webhook: {}", response.text().await?);
    }

    Ok(())
}
