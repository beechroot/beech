use anyhow::{bail, Result};
use clap::{Parser, ValueHint};
use log::{error, info};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use simple_logger::SimpleLogger;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::SystemTime;
use tokio::process::Command;
use tokio::sync::mpsc;
use url::Url;

#[derive(Parser, Debug)]
struct Args {
    /// Directory to watch for request files
    #[arg(short, long, value_hint = ValueHint::DirPath)]
    dir: PathBuf,

    /// Base URL for fetching pages
    #[arg(short, long)]
    url: Option<Url>,

    /// Path or command name of wget executable
    #[arg(long, default_value = "wget", value_hint = ValueHint::FilePath)]
    wget: PathBuf,
}

async fn wget(wget_command: &Path, base_url: &Url, dir: &Path, suffix: &str) -> Result<()> {
    let full_url = base_url.join(suffix)?;
    let output_path = dir.join(format!("response.{}", suffix));

    let status = Command::new(wget_command)
        .arg("-q") // quiet mode: no output
        .arg("--no-clobber") // just in case (won't overwrite if already exists)
        .arg("-O")
        .arg(&output_path)
        .arg(full_url.as_str())
        .stdout(Stdio::null()) // discard stdout
        .stderr(Stdio::null()) // discard stderr
        .status()
        .await?;

    if !status.success() {
        bail!(
            "'{:?}' failed with exit code {} while downloading {}",
            wget_command,
            status.code().unwrap_or(-1),
            full_url
        );
    }

    Ok(())
}

fn should_handle_request(path: &Path, dir: &Path) -> Option<(PathBuf, String)> {
    if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
        if file_name.starts_with("request.") {
            if let Some(suffix) = file_name.strip_prefix("request.") {
                let suffix = suffix.to_string();
                let dir = dir.to_path_buf();
                return Some((dir, suffix));
            }
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    SimpleLogger::new().env().init().unwrap();

    if args.url.is_some() {
        if which::which(&args.wget).is_err() {
            error!("--url was provided, but 'wget' is not installed or not found in PATH.");
            std::process::exit(1);
        }
    }

    let (tx, mut rx) = mpsc::channel::<(SystemTime, Event)>(100);
    let watch_dir = args.dir.clone();

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            if let Ok(event) = res {
                let _ = tx.blocking_send((SystemTime::now(), event));
            }
        },
        notify::Config::default(),
    )?;

    watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

    info!("Watching directory: {}", watch_dir.display());

    // Process pre-existing request.* files
    for entry in std::fs::read_dir(watch_dir.as_path())? {
        let entry = entry?;
        let path = entry.path();
        let url = args.url.clone();
        let wget = args.wget.clone();
        if let Some((dir, suffix)) = should_handle_request(path.as_path(), watch_dir.as_path()) {
            tokio::spawn(async move {
                if let Err(e) =
                    handle_request(&dir, &suffix, &url, wget.as_path(), SystemTime::now()).await
                {
                    error!("Error handling request.{}: {:?}", suffix, e);
                }
            });
        }
    }

    let shutdown = tokio::spawn(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c");
        info!("Received SIGINT. Shutting down...");
    });

    let handler = tokio::spawn(async move {
        while let Some((recv_time, event)) = rx.recv().await {
            if matches!(event.kind, EventKind::Create(_)) {
                for path in event.paths {
                    if let Some((dir, suffix)) =
                        should_handle_request(path.as_path(), watch_dir.as_path())
                    {
                        let url = args.url.clone();
                        let wget = args.wget.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                handle_request(&dir, &suffix, &url, wget.as_path(), recv_time).await
                            {
                                error!("Error handling request.{}: {:?}", suffix, e);
                            }
                        });
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = shutdown => (),
        _ = handler => (),
    }

    Ok(())
}

async fn handle_request(
    dir: &PathBuf,
    suffix: &str,
    maybe_url: &Option<Url>,
    wget_path: &Path,
    recv_time: SystemTime,
) -> Result<()> {
    let request_file = dir.join(format!("request.{}", suffix));
    let response_file = dir.join(format!("response.{}", suffix));
    let final_file = dir.join(suffix);

    // Attempt to delete the request file first
    match std::fs::remove_file(&request_file) {
        Ok(_) => {
            // Check if final file already exists
            if final_file.exists() {
                #[rustfmt::skip] info!("Skipping request.{}: {} already exists", suffix, final_file.display());
                return Ok(());
            }

            if let Some(u) = maybe_url.as_ref() {
                let _ = wget(wget_path, u, dir.as_path(), suffix).await?;
            }

            // Try to rename, but ignore AlreadyExists error
            match std::fs::rename(&response_file, &final_file) {
                Ok(_) => {
                    let took = SystemTime::now().duration_since(recv_time);
                    #[rustfmt::skip] info!("Handled request.{} → {} in {}", suffix, final_file.display(),took.map(|d| format!("{:?}", d)).unwrap_or("???".to_string())
                    );
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    #[rustfmt::skip] info!("Request.{}: {} already existed during rename", suffix, final_file.display());
                    // Optionally clean up stale response file
                    let _ = std::fs::remove_file(&response_file);
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) => {
            #[rustfmt::skip] error!("Skipping request.{}: failed to delete {} ({})", suffix, request_file.display(), e);
        }
    }

    Ok(())
}
