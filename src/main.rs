use anyhow::Result;
use cbzxl::{api, config::Config, db::Database, worker};
use clap::{Parser, Subcommand};
use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Option<std::fs::File>>>);

struct TeeWriter(Arc<Mutex<Option<std::fs::File>>>);

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = TeeWriter;
    fn make_writer(&'a self) -> Self::Writer {
        TeeWriter(self.0.clone())
    }
}

impl Write for TeeWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        std::io::stdout().write_all(buffer)?;
        if let Ok(mut file) = self.0.lock()
            && let Some(file) = file.as_mut()
        {
            file.write_all(buffer)?;
        }
        Ok(buffer.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()?;
        if let Ok(mut file) = self.0.lock()
            && let Some(file) = file.as_mut()
        {
            file.flush()?;
        }
        Ok(())
    }
}

#[derive(Parser)]
#[command(version, about = "Safe, self-hosted CBZ to JPEG XL optimization")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Run,
    Serve,
    Worker,
    Migrate {
        source: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    Doctor,
    Healthcheck,
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_file = std::env::var_os("LOG_FILE")
        .filter(|value| !value.is_empty())
        .map(|path| OpenOptions::new().create(true).append(true).open(path))
        .transpose()?;
    tracing_subscriber::fmt()
        .with_writer(LogWriter(Arc::new(Mutex::new(log_file))))
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "cbzxl=info".into()))
        .init();

    let cli = Cli::parse();
    let config = Config::from_env()?;
    let command = cli.command.unwrap_or(Command::Run);
    if matches!(command, Command::Healthcheck) {
        let response = reqwest::Client::new()
            .get(format!(
                "http://{}/ready",
                healthcheck_address(&config.listen)
            ))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "CBZXL is not ready");
        return Ok(());
    }
    let database = Database::open(config.database.clone())?;
    let migration_error = database.migrate().err().map(|error| error.to_string());

    match command {
        Command::Run => api::serve(config, database, true, migration_error).await,
        Command::Serve => api::serve(config, database, false, migration_error).await,
        Command::Worker => {
            if let Some(error) = migration_error {
                anyhow::bail!(error);
            }
            worker::run(config, database).await
        }
        Command::Migrate { source, dry_run } => {
            if let Some(error) = migration_error {
                anyhow::bail!(error);
            }
            let report = database.import(&source, dry_run)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::Doctor => {
            if let Some(error) = migration_error {
                anyhow::bail!(error);
            }
            println!("{}", serde_json::to_string_pretty(&database.doctor()?)?);
            Ok(())
        }
        Command::Healthcheck => unreachable!(),
    }
}

fn healthcheck_address(listen: &str) -> String {
    let port = listen.rsplit_once(':').map_or("5000", |(_, port)| port);
    format!("127.0.0.1:{port}")
}
