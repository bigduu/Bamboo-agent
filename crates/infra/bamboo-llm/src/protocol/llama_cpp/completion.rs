use std::{net::SocketAddr, time::Duration};

use bamboo_domain::{
    patrol_model_limits::{MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS},
    TokenUsage,
};
use reqwest::{Client, Request, RequestBuilder, Response, Url};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

use super::{tokenizer, GptOssPrompt, END, MESSAGE, START};

const MODEL: &str = "gpt-oss-20b";
const CHANNEL: u32 = 200_005;
const RETURN: u32 = 200_002;
const FIRST_SPECIAL: u32 = 199_998;
const ASSISTANT: u32 = 173_781;
const RESPONSE_BYTES: usize = 128 * 1024;
const PROBE: &str = "<|start|>assistant<|channel|>final<|message|><|return|>";
const PROBE_TOKENS: &[u32] = &[START, ASSISTANT, CHANNEL, 17_196, MESSAGE, RETURN];

#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    #[error("invalid local completion endpoint or limits")]
    InvalidRequest,
    #[error("the local deployment does not match the gpt-oss-20b raw profile")]
    DeploymentMismatch,
    #[error("the local completion request failed or expired")]
    Unavailable,
    #[error("the local completion response was invalid or too large")]
    InvalidResponse,
}

/// Read-only outcome and separately observed token counts. Transport failures
/// carry no usage proof. Malformed or missing advice can still have known usage.
pub struct GptOssCompletion {
    pub final_text: Option<String>,
    pub usage: CompletionUsage,
}

/// Independently observed counts. A missing dimension prevents a refund but
/// cannot erase proof that the other dimension exceeded its ceiling.
pub struct CompletionUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

impl CompletionUsage {
    pub fn known(&self) -> Option<TokenUsage> {
        let (prompt_tokens, completion_tokens) = (self.input_tokens?, self.output_tokens?);
        Some(TokenUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens.checked_add(completion_tokens)?,
        })
    }
}

/// Dedicated client for an explicitly trusted, single-model loopback server.
pub struct GptOssLocalClient {
    http: Client,
    origin: Url,
}

impl GptOssLocalClient {
    pub fn new(address: SocketAddr) -> Result<Self, CompletionError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(CompletionError::InvalidRequest);
        }
        let origin = Url::parse(&format!("http://{address}/"))
            .map_err(|_| CompletionError::InvalidRequest)?;
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|_| CompletionError::Unavailable)?;
        Ok(Self { http, origin })
    }

    fn url(&self, path: &str) -> Url {
        let mut url = self.origin.clone();
        url.set_path(path);
        url
    }

    /// Check profile compatibility without sending the prompt or generating
    /// tokens. The Host must still admit the exact request before sending it.
    pub async fn prepare(
        &self,
        prompt: &GptOssPrompt,
        max_output_tokens: u32,
        timeout: Duration,
    ) -> Result<PreparedCompletion, CompletionError> {
        let input_tokens =
            u32::try_from(prompt.tokens().len()).map_err(|_| CompletionError::InvalidRequest)?;
        if !(1..=MAX_INPUT_TOKENS).contains(&input_tokens)
            || !(1..=MAX_OUTPUT_TOKENS).contains(&max_output_tokens)
            || timeout.is_zero()
        {
            return Err(CompletionError::InvalidRequest);
        }
        tokio::time::timeout(
            timeout,
            self.check_profile(input_tokens + max_output_tokens),
        )
        .await
        .map_err(|_| CompletionError::Unavailable)??;
        let request = self
            .http
            .post(self.url("/completion"))
            .json(&json!({
                "model": MODEL,
                "prompt": prompt.tokens(),
                "n_predict": max_output_tokens,
                "n_cmpl": 1,
                "n_keep": -1,
                "cache_prompt": false,
                "stream": false,
                "return_tokens": true,
                "ignore_eos": false,
                "stop": [],
                "response_fields": [
                    "model", "stop", "stop_type", "truncated", "tokens",
                    "tokens_evaluated", "tokens_predicted"
                ],
            }))
            .build()
            .map_err(|_| CompletionError::InvalidRequest)?;
        Ok(PreparedCompletion {
            http: self.http.clone(),
            request,
            input_tokens,
            max_output_tokens,
        })
    }

    async fn check_profile(&self, needed: u32) -> Result<(), CompletionError> {
        let models: Models = read_json(self.http.get(self.url("/v1/models"))).await?;
        let [model] = models.data.as_slice() else {
            return Err(CompletionError::DeploymentMismatch);
        };
        // A model alias is operator-controlled. Check the model class and the
        // vocabulary as well, including the 20B versus 120B parameter class.
        if model.id != MODEL
            || model.owned_by != "llamacpp"
            || model.meta.n_vocab != 201_088
            || model.meta.n_embd != 2880
            || !(20_000_000_000..22_000_000_000).contains(&model.meta.n_params)
        {
            return Err(CompletionError::DeploymentMismatch);
        }
        let props: Props = read_json(self.http.get(self.url("/props"))).await?;
        if props.model_alias != MODEL || needed >= props.default_generation_settings.n_ctx {
            return Err(CompletionError::DeploymentMismatch);
        }
        let probe: Tokenized = read_json(self.http.post(self.url("/tokenize")).json(&json!({
            "content": PROBE, "add_special": false, "parse_special": true
        })))
        .await?;
        if probe.tokens != PROBE_TOKENS {
            return Err(CompletionError::DeploymentMismatch);
        }
        Ok(())
    }
}

/// One consuming submission of a prepared request. Dropping it sends nothing.
pub struct PreparedCompletion {
    http: Client,
    request: Request,
    input_tokens: u32,
    max_output_tokens: u32,
}

impl PreparedCompletion {
    pub async fn complete(
        mut self,
        timeout: Duration,
    ) -> Result<GptOssCompletion, CompletionError> {
        if timeout.is_zero() {
            return Err(CompletionError::InvalidRequest);
        }
        *self.request.timeout_mut() = Some(timeout);
        let response = self
            .http
            .execute(self.request)
            .await
            .map_err(|_| CompletionError::Unavailable)?;
        let completion: WireCompletion = response_json(response).await?;
        if completion.model != MODEL || !completion.stop {
            return Err(CompletionError::InvalidResponse);
        }
        let usage = CompletionUsage {
            input_tokens: completion
                .tokens_evaluated
                .as_u64()
                .filter(|&input| input >= u64::from(self.input_tokens)),
            output_tokens: completion.tokens_predicted.as_u64(),
        };
        let final_text = match usage.known() {
            Some(counts)
                if counts.prompt_tokens == u64::from(self.input_tokens)
                    && counts.completion_tokens <= u64::from(self.max_output_tokens)
                    && completion.stop_type.as_str() == Some("eos")
                    && completion.truncated.as_bool() == Some(false) =>
            {
                let tokens: Option<Vec<u32>> = serde_json::from_value(completion.tokens).ok();
                tokens.and_then(|tokens| {
                    (tokens.len() as u64 == counts.completion_tokens)
                        .then(|| decode_final(&tokens))
                        .flatten()
                })
            }
            _ => None,
        };
        Ok(GptOssCompletion { final_text, usage })
    }
}

fn decode_final(mut tokens: &[u32]) -> Option<String> {
    let tokenizer = tokenizer().ok()?;
    loop {
        tokens = tokens.strip_prefix(&[CHANNEL])?;
        let message = tokens.iter().position(|&id| id == MESSAGE)?;
        let (channel, body) = tokens.split_at(message);
        let body = &body[1..];
        let end = body
            .iter()
            .position(|&id| id >= FIRST_SPECIAL)
            .unwrap_or(body.len());
        let (text, tail) = body.split_at(end);
        match tokenizer.decode(channel).ok()?.as_str() {
            "analysis" => tokens = tail.strip_prefix(&[END, START, ASSISTANT])?,
            "final" if tail == [RETURN] => {
                return tokenizer.decode(text).ok().filter(|s| !s.is_empty());
            }
            _ => return None,
        }
    }
}

async fn read_json<T: DeserializeOwned>(request: RequestBuilder) -> Result<T, CompletionError> {
    let response = request
        .send()
        .await
        .map_err(|_| CompletionError::Unavailable)?;
    response_json(response).await
}

async fn response_json<T: DeserializeOwned>(mut response: Response) -> Result<T, CompletionError> {
    if !response.status().is_success() {
        return Err(CompletionError::Unavailable);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| CompletionError::Unavailable)?
    {
        if chunk.len() > RESPONSE_BYTES - bytes.len() {
            return Err(CompletionError::InvalidResponse);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| CompletionError::InvalidResponse)
}

#[derive(Deserialize)]
struct Models {
    data: Vec<ModelEntry>,
}

#[derive(Deserialize)]
struct ModelEntry {
    id: String,
    owned_by: String,
    meta: ModelMeta,
}

#[derive(Deserialize)]
struct ModelMeta {
    n_vocab: u32,
    n_embd: u32,
    n_params: u64,
}

#[derive(Deserialize)]
struct Props {
    model_alias: String,
    default_generation_settings: GenerationSettings,
}

#[derive(Deserialize)]
struct GenerationSettings {
    n_ctx: u32,
}

#[derive(Deserialize)]
struct Tokenized {
    tokens: Vec<u32>,
}

#[derive(Deserialize)]
struct WireCompletion {
    model: String,
    stop: bool,
    #[serde(default)]
    stop_type: Value,
    #[serde(default)]
    truncated: Value,
    #[serde(default)]
    tokens: Value,
    #[serde(default)]
    tokens_evaluated: Value,
    #[serde(default)]
    tokens_predicted: Value,
}

#[cfg(test)]
mod tests;
