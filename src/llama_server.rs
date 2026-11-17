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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 18117,
            model_alias: "arkhiva-qwen".into(),
            server_executable: None,
            model_file: None,
            runtime_library_dir: None,
        }
    }
}

pub(crate) struct LlamaServer {
    client: Client,
    base: String,
    model: String,
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

    pub(crate) fn chat(&self, mut request: Value) -> Result<Value> {
        request["model"] = Value::String(self.model.clone());
        let response = self
            .client
            .post(format!("{}/v1/chat/completions", self.base))
            .header("Content-Type", "application/json")
            .body(request.to_string())
            .send()
            .context("requesting structured JSON from local llama-server")?;
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

fn read_json(response: Response) -> Result<Value> {
    let status = response.status();
    let mut body = Vec::new();
    response.take(65537).read_to_end(&mut body)?;
    ensure!(body.len() <= 65536, "llama-server response is too large");
    ensure!(status.is_success(), "llama-server returned HTTP {status}");
    serde_json::from_slice(&body).context("invalid llama-server JSON response")
}
