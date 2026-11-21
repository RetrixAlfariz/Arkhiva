use anyhow::{Context, Result, bail, ensure};
use reqwest::blocking::{Client, Response};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const READY_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    port: u16,
    model_alias: String,
    server_executable: Option<PathBuf>,
    model_file: Option<PathBuf>,
    runtime_library_dir: Option<PathBuf>,
    // An OpenAI-compatible API; `ARKHIVA_LLM_*` variables override these. The key is never
    // read from this file, so `api_key` is rejected as an unknown field.
    api_url: Option<String>,
    api_model: Option<String>,
    api_reasoning: Option<String>,
    api_temperature: Option<f64>,
    /// "api" or "local"; unset means the API when `api_url` is set, else the local server.
    llm: Option<Llm>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Llm {
    Api,
    Local,
}

impl Config {
    /// The API URL to call, or `None` for the local llama-server. `ARKHIVA_LLM_URL` wins.
    fn api_url(&self, env_url: Option<String>) -> Result<Option<String>> {
        match (env_url, self.llm) {
            (Some(url), _) => Ok(Some(url)),
            (None, Some(Llm::Local)) => Ok(None),
            (None, Some(Llm::Api)) => self
                .api_url
                .clone()
                .map(Some)
                .context("\"llm\": \"api\" requires api_url or ARKHIVA_LLM_URL"),
            (None, None) => Ok(self.api_url.clone()),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 18117,
            model_alias: "arkhiva-qwen".into(),
            server_executable: None,
            model_file: None,
            runtime_library_dir: None,
            api_url: None,
            api_model: None,
            api_reasoning: None,
            api_temperature: None,
            llm: None,
        }
    }
}

pub(crate) struct LlamaServer {
    client: Client,
    base: String,
    model: String,
    remote: Option<Remote>,
}

/// An OpenAI-compatible API chosen with `ARKHIVA_LLM_URL` or `api_url`; the key stays in the
/// environment.
struct Remote {
    key: Option<String>,
    temperature: Option<f64>,
    reasoning: Option<String>,
}

impl LlamaServer {
    pub(crate) fn connect(corpus: &Path) -> Result<Self> {
        let config_path = corpus.join("arkhiva-llama.json");
        let config: Config = match fs::read(&config_path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("reading {}", config_path.display()))?,
            Err(error) if error.kind() == ErrorKind::NotFound => Config::default(),
            Err(error) => return Err(error.into()),
        };
        if let Some(url) = config.api_url(env("ARKHIVA_LLM_URL"))? {
            return Self::remote(&url, &config);
        }
        ensure!(config.port != 0, "llama-server port must be nonzero");
        ensure!(
            !config.model_alias.trim().is_empty(),
            "llama-server model_alias must not be empty"
        );
        ensure!(
            config.server_executable.is_some() == config.model_file.is_some(),
            "configure both server_executable and model_file for automatic startup"
        );
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));
        let server = Self {
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(45))
                .build()?,
            base: format!("http://{address}"),
            model: config.model_alias.clone(),
            remote: None,
        };

        // Each CLI process may start the server. Serialize startup by port.
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(std::env::temp_dir().join(format!("arkhiva-llama-{}.lock", config.port)))?;
        lock.lock()?;
        let mut launched =
            if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
                None
            } else {
                Some(server.start(corpus, &config, address)?)
            };

        let model = config.model_file.as_ref().map(|file| corpus.join(file));
        if let Err(error) = server.wait_ready(launched.as_mut(), model.as_deref()) {
            if let Some(mut child) = launched {
                let _ = child.kill();
                let _ = child.wait();
            }
            return Err(error);
        }
        Ok(server)
    }

    fn start(&self, corpus: &Path, config: &Config, address: SocketAddr) -> Result<Child> {
        let (Some(exe), Some(model)) = (&config.server_executable, &config.model_file) else {
            bail!(
                "no llama-server at {}; configure automatic startup in {} or start it on that port with alias {}",
                self.base,
                corpus.join("arkhiva-llama.json").display(),
                self.model,
            );
        };
        let exe = std::path::absolute(corpus.join(exe))?;
        let model = std::path::absolute(corpus.join(model))?;
        ensure!(
            exe.is_file(),
            "missing llama-server executable {}",
            exe.display()
        );
        ensure!(model.is_file(), "missing GGUF model {}", model.display());
        let log_path = corpus.join("arkhiva-llama.log");
        let log = File::create(&log_path)?;
        let mut command = Command::new(&exe);
        if let Some(directory) = &config.runtime_library_dir {
            let directory = std::path::absolute(corpus.join(directory))?;
            ensure!(
                directory.is_dir(),
                "missing runtime library directory {}",
                directory.display()
            );
            let current_path = std::env::var_os("PATH").unwrap_or_default();
            let path = std::env::join_paths(
                std::iter::once(directory).chain(std::env::split_paths(&current_path)),
            )?;
            command.env("PATH", path);
        }
        command
            .arg("--model")
            .arg(model)
            .args(["--host", "127.0.0.1", "--port", &address.port().to_string()])
            .args([
                "--alias",
                &self.model,
                "--ctx-size",
                "8192",
                "--parallel",
                "1",
            ])
            .args([
                "--gpu-layers",
                "auto",
                "--reasoning",
                "off",
                "--no-mmproj",
                "--no-webui",
            ])
            .args([
                "--sleep-idle-seconds",
                "300",
                "--cache-ram",
                "128",
                "--log-verbosity",
                "2",
            ])
            .args(["--cors-origins", &self.base])
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        eprintln!("Starting local llama-server…");
        spawn_server(&mut command).with_context(|| format!("starting {}", exe.display()))
    }

    fn wait_ready(&self, mut child: Option<&mut Child>, model: Option<&Path>) -> Result<()> {
        let started = Instant::now();
        loop {
            if let Some(child) = child.as_deref_mut()
                && let Some(status) = child.try_wait()?
            {
                bail!(
                    "llama-server exited ({status}); see arkhiva-llama.log in the corpus directory"
                );
            }
            match self
                .client
                .get(format!("{}/health", self.base))
                .timeout(Duration::from_secs(1))
                .send()
            {
                Ok(response) if response.status().is_success() => break,
                Ok(response) if response.status().as_u16() == 503 => {}
                Ok(response) => bail!(
                    "llama-server health returned HTTP {} at {}",
                    response.status(),
                    self.base
                ),
                Err(error) if child.is_some() && (error.is_connect() || error.is_timeout()) => {}
                Err(error) => return Err(error).context("checking local llama-server health"),
            }
            ensure!(
                started.elapsed() < READY_TIMEOUT,
                "llama-server did not become ready at {}; check arkhiva-llama.log",
                self.base
            );
            thread::sleep(Duration::from_millis(150));
        }
        let models = read_json(self.client.get(format!("{}/v1/models", self.base)).send()?)?;
        ensure!(
            models["data"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| entry["id"] == self.model)),
            "llama-server at {} does not expose model {}",
            self.base,
            self.model,
        );
        if let Some(model) = model {
            let props = read_json(self.client.get(format!("{}/props", self.base)).send()?)?;
            let loaded = props["model_path"]
                .as_str()
                .context("llama-server did not report its GGUF path")?;
            ensure!(
                fs::canonicalize(loaded)? == fs::canonicalize(model)?,
                "llama-server at {} loaded a different GGUF; stop it or choose another port",
                self.base,
            );
        }
        Ok(())
    }

    fn remote(url: &str, config: &Config) -> Result<Self> {
        let url = url.trim_end_matches('/');
        // The key is sent as a bearer token, so plain http is allowed only to this machine.
        let parsed = reqwest::Url::parse(url).context("the LLM API URL is not a valid URL")?;
        let loopback = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        ensure!(
            parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback),
            "the LLM API URL must use https (plain http only for localhost)"
        );
        let model = env("ARKHIVA_LLM_MODEL")
            .or_else(|| config.api_model.clone())
            .context("an LLM API URL requires ARKHIVA_LLM_MODEL or api_model")?;
        let temperature = match env("ARKHIVA_LLM_TEMPERATURE") {
            Some(value) => Some(
                value
                    .parse()
                    .context("ARKHIVA_LLM_TEMPERATURE must be a number")?,
            ),
            None => config.api_temperature,
        };
        Ok(Self {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(60))
                .build()?,
            base: url.to_owned(),
            model,
            remote: Some(Remote {
                key: env("ARKHIVA_LLM_KEY").or_else(|| env("OPENAI_API_KEY")),
                temperature,
                reasoning: env("ARKHIVA_LLM_REASONING").or_else(|| config.api_reasoning.clone()),
            }),
        })
    }

    pub(crate) fn chat(&self, mut request: Value, max_tokens: u32) -> Result<Value> {
        request["model"] = Value::String(self.model.clone());
        let mut post = match &self.remote {
            None => {
                request["temperature"] = 0.0.into();
                request["max_tokens"] = max_tokens.into();
                request["reasoning_effort"] = "none".into();
                request["chat_template_kwargs"] = serde_json::json!({"enable_thinking": false});
                self.client
                    .post(format!("{}/v1/chat/completions", self.base))
            }
            Some(remote) => {
                // Newer OpenAI models reject max_tokens, and reasoning tokens count toward the cap.
                request["max_completion_tokens"] = (max_tokens * 5).into();
                if let Some(temperature) = remote.temperature {
                    request["temperature"] = temperature.into();
                }
                if let Some(reasoning) = &remote.reasoning {
                    request["reasoning_effort"] = reasoning.as_str().into();
                }
                let post = self.client.post(format!("{}/chat/completions", self.base));
                match &remote.key {
                    Some(key) => post.bearer_auth(key),
                    None => post,
                }
            }
        };
        post = post.header("Content-Type", "application/json");
        let response = post
            .body(request.to_string())
            .send()
            .with_context(|| format!("requesting a search plan from {}", self.base))?;
        read_json(response)
    }
}

fn spawn_server(command: &mut Command) -> std::io::Result<Child> {
    #[cfg(windows)]
    let _std_handles = windows::separate_stdio(command)?;
    command.spawn()
}

#[cfg(windows)]
mod windows {
    use std::{io, os::windows::process::CommandExt, process::Command};
    use windows_sys::Win32::{
        Foundation::{
            GetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE,
            SetHandleInformation,
        },
        System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE},
    };

    pub(super) fn separate_stdio(command: &mut Command) -> io::Result<StdHandles> {
        command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        let mut saved = StdHandles(Vec::new());
        // CreateProcess can inherit other inheritable handles besides the child's redirected
        // streams. Keep the caller's capture pipes out of the long-lived server.
        for stream in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
            let handle = unsafe { GetStdHandle(stream) };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                continue;
            }
            let mut flags = 0;
            if unsafe { GetHandleInformation(handle, &mut flags) } != 0
                && flags & HANDLE_FLAG_INHERIT != 0
            {
                if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                saved.0.push(handle);
            }
        }
        Ok(saved)
    }

    pub(super) struct StdHandles(Vec<HANDLE>);

    impl Drop for StdHandles {
        fn drop(&mut self) {
            for &handle in &self.0 {
                unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) };
            }
        }
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn read_json(response: Response) -> Result<Value> {
    let status = response.status();
    let mut body = Vec::new();
    response.take(65537).read_to_end(&mut body)?;
    ensure!(body.len() <= 65536, "LLM response is too large");
    // An API error body names the reason (unknown model, unsupported field); it holds no key.
    ensure!(
        status.is_success(),
        "LLM returned HTTP {status}: {}",
        String::from_utf8_lossy(&body[..body.len().min(500)])
    );
    serde_json::from_slice(&body).context("invalid llama-server JSON response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_holds_api_settings_but_never_a_key() {
        let config: Config = serde_json::from_str(
            r#"{"api_url": "https://api.example.com/v1/", "api_model": "m", "api_reasoning": "none", "api_temperature": 0}"#,
        )
        .unwrap();
        assert_eq!(config.port, 18117);
        assert_eq!(config.api_temperature, Some(0.0));
        let server = LlamaServer::remote(config.api_url.as_deref().unwrap(), &config).unwrap();
        assert_eq!(server.base, "https://api.example.com/v1");
        assert!(serde_json::from_str::<Config>(r#"{"api_key": "x"}"#).is_err());
        assert!(LlamaServer::remote("http://api.example.com", &config).is_err());
        for url in [
            "http://localhost.evil.com/v1",
            "http://127.0.0.1.evil.com/v1",
            "http://localhost@evil.com/v1",
            "http://127.0.0.1@evil.com/v1",
            "ftp://localhost/v1",
        ] {
            assert!(LlamaServer::remote(url, &config).is_err(), "{url}");
        }
        for url in [
            "http://localhost:8080/v1",
            "http://127.0.0.1/v1",
            "http://[::1]:9/v1",
        ] {
            assert!(LlamaServer::remote(url, &config).is_ok(), "{url}");
        }

        let with = |llm: &str| -> Config {
            serde_json::from_str(&format!(r#"{{"api_url": "https://a/v1", "llm": {llm}}}"#))
                .unwrap()
        };
        assert_eq!(
            with("null").api_url(None).unwrap().as_deref(),
            Some("https://a/v1")
        );
        assert_eq!(
            with(r#""api""#).api_url(None).unwrap().as_deref(),
            Some("https://a/v1")
        );
        assert_eq!(with(r#""local""#).api_url(None).unwrap(), None);
        let env_url = Some("http://env/v1".to_owned());
        assert_eq!(
            with(r#""local""#).api_url(env_url).unwrap().as_deref(),
            Some("http://env/v1")
        );
        let api_only: Config = serde_json::from_str(r#"{"llm": "api"}"#).unwrap();
        assert!(api_only.api_url(None).is_err());
        assert!(serde_json::from_str::<Config>(r#"{"llm": "cloud"}"#).is_err());
    }
}
