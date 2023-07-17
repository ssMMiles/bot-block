use reqwest::{
    header::{HeaderMap, HeaderValue},
    Response,
};
use serde_json::json;

use crate::botblock::BotBlock;

pub struct GrafanaRender {
    pub user_id: String,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
}

impl BotBlock {
    fn get_grafana_uri(&self, details: GrafanaRender) -> String {
        format!(
            "{}&var-targetUser={}&from={}000&to={}000",
            self.grafana_uri_base, details.user_id, details.start_timestamp, details.end_timestamp
        )
    }

    pub async fn send_webhook(
        &self,
        message: &str,
        render_details: Option<GrafanaRender>,
    ) -> Result<(), reqwest::Error> {
        let response: Response;
        if let Some(details) = render_details {
            let image = self
                .http_client
                .get(self.get_grafana_uri(details))
                .header("Authorization", self.grafana_api_token.clone())
                .send()
                .await?
                .bytes()
                .await?;

            let message_data = json!({
                "embeds": [
                    {
                        "title": "BotBlock Detection",
                        "description": message,
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

            response = self
                .http_client
                .post(&self.discord_webhook_url)
                .multipart(form)
                .send()
                .await?;
        } else {
            let message_data = json!({
                "embeds": [
                    {
                        "title": "BotBlock Detection",
                        "description": message,
                    }
                ]
            });

            response = self
                .http_client
                .post(&self.discord_webhook_url)
                .json(&message_data)
                .send()
                .await?;
        }

        if !response.status().is_success() {
            log::error!("Failed to send webhook: {}", response.text().await?);
        }

        Ok(())
    }
}
