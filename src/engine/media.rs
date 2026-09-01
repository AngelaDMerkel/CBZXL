use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    process::Command as StdCommand,
    sync::LazyLock,
    time::Duration,
};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct MediaRequest {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub effort: u8,
    pub distance: f32,
    pub threads: usize,
}

#[derive(Clone, Debug)]
pub struct MediaCandidate {
    pub path: PathBuf,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct ImageInfo {
    pub width: u32,
    pub height: u32,
    pub colorspace: String,
}

impl ImageInfo {
    pub fn megapixels(&self) -> f32 {
        self.width as f32 * self.height as f32 / 1_000_000.0
    }
}

#[async_trait]
pub trait MediaBackend: Send + Sync {
    fn doctor(&self) -> Result<()>;
    async fn info(&self, source: &Path) -> Result<ImageInfo>;
    async fn normalize(&self, source: &Path, destination: &Path) -> Result<()>;
    async fn signature(&self, source: &Path) -> Result<String>;
    async fn encode(&self, request: MediaRequest) -> Result<MediaCandidate>;
    async fn verify(
        &self,
        _source: &Path,
        candidate: &Path,
        lossless: bool,
        reconstruct_jpeg: bool,
        source_signature: Option<&str>,
    ) -> Result<()>;
    async fn decode_jxl(&self, source: &Path) -> Result<()>;
}

#[derive(Clone)]
pub struct CommandMediaBackend {
    encoder_timeout: Duration,
    auxiliary_timeout: Duration,
}

impl CommandMediaBackend {
    pub fn new(encoder_timeout: Duration, auxiliary_timeout: Duration) -> Self {
        Self {
            encoder_timeout,
            auxiliary_timeout,
        }
    }
}

impl Default for CommandMediaBackend {
    fn default() -> Self {
        Self::new(Duration::ZERO, Duration::from_secs(300))
    }
}

#[async_trait]
impl MediaBackend for CommandMediaBackend {
    fn doctor(&self) -> Result<()> {
        for command in ["cjxl", "djxl", "identify"] {
            ensure!(
                which(command),
                "required media tool is unavailable: {command}"
            );
        }
        ensure!(which(image_tool()), "ImageMagick is unavailable");
        for command in ["cjxl", "djxl"] {
            let version = StdCommand::new(command).arg("--version").output()?;
            let text = format!(
                "{} {}",
                String::from_utf8_lossy(&version.stdout),
                String::from_utf8_lossy(&version.stderr)
            );
            ensure!(
                parse_version(&text).is_some_and(|version| version >= (0, 12, 0)),
                "{command} 0.12.0 or newer is required"
            );
        }
        runtime_probe()?;
        Ok(())
    }

    async fn info(&self, source: &Path) -> Result<ImageInfo> {
        let mut command = Command::new("identify");
        command.args(["-format", "%w %h %[colorspace]"]).arg(source);
        let output = async_output(command, self.auxiliary_timeout).await?;
        ensure!(
            output.status.success(),
            "could not inspect {}",
            source.display()
        );
        let text = String::from_utf8(output.stdout)?;
        let mut values = text.split_whitespace();
        Ok(ImageInfo {
            width: values.next().context("image width is missing")?.parse()?,
            height: values.next().context("image height is missing")?.parse()?,
            colorspace: values.collect::<Vec<_>>().join(" "),
        })
    }

    async fn normalize(&self, source: &Path, destination: &Path) -> Result<()> {
        let mut command = Command::new(image_tool());
        command
            .arg(source)
            .args(["-auto-orient", "-colorspace", "sRGB", "-strip"])
            .arg(destination);
        let output = async_output(command, self.auxiliary_timeout).await?;
        if !output.status.success() {
            let recovered = destination
                .metadata()
                .is_ok_and(|metadata| metadata.len() > 0)
                && self.info(destination).await.is_ok();
            ensure!(
                recovered,
                "normalization failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        ensure!(
            destination.metadata()?.len() > 0,
            "normalization produced an empty image"
        );
        Ok(())
    }

    async fn signature(&self, source: &Path) -> Result<String> {
        let source = source.to_owned();
        tokio::task::spawn_blocking(move || pixel_signature(&source)).await?
    }

    async fn encode(&self, request: MediaRequest) -> Result<MediaCandidate> {
        let mut command = Command::new("cjxl");
        encoder_args(&mut command, &request, false);
        let mut output = async_output(command, self.encoder_timeout)
            .await
            .context("could not run cjxl")?;
        if !output.status.success()
            && String::from_utf8_lossy(&output.stderr)
                .contains("JPEG bitstream reconstruction data could not be created")
        {
            let _ = std::fs::remove_file(&request.destination);
            let mut retry = Command::new("cjxl");
            encoder_args(&mut retry, &request, true);
            output = async_output(retry, self.encoder_timeout).await?;
        }
        ensure!(
            output.status.success(),
            "cjxl failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        let size = request.destination.metadata()?.len();
        ensure!(size > 0, "cjxl produced an empty candidate");
        Ok(MediaCandidate {
            path: request.destination,
            size,
        })
    }

    async fn verify(
        &self,
        _source: &Path,
        candidate: &Path,
        lossless: bool,
        reconstruct_jpeg: bool,
        source_signature: Option<&str>,
    ) -> Result<()> {
        let directory = candidate.parent().context("candidate has no parent")?;
        let decoded = tempfile::Builder::new()
            .suffix(if reconstruct_jpeg { ".jpg" } else { ".png" })
            .tempfile_in(directory)?;
        let mut command = Command::new("djxl");
        command
            .arg(candidate)
            .arg(decoded.path())
            .args(["--num_threads=1", "--quiet"]);
        let output = async_output(command, self.auxiliary_timeout).await?;
        ensure!(
            output.status.success(),
            "djxl failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        if lossless {
            let expected = source_signature
                .context("lossless source signature is unavailable")?
                .to_owned();
            let decoded = decoded.path().to_owned();
            tokio::task::spawn_blocking(move || -> Result<()> {
                ensure!(
                    expected == pixel_signature(&decoded)?,
                    "lossless pixel verification failed"
                );
                Ok(())
            })
            .await??;
        }
        Ok(())
    }

    async fn decode_jxl(&self, source: &Path) -> Result<()> {
        let mut command = Command::new("djxl");
        command
            .arg(source)
            .args(["--disable_output", "--num_threads=1", "--quiet"]);
        let output = async_output(command, self.auxiliary_timeout).await?;
        ensure!(
            output.status.success(),
            "JPEG XL decode failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(())
    }
}

fn encoder_args(command: &mut Command, request: &MediaRequest, disable_reconstruction: bool) {
    command.args([
        "-d",
        &request.distance.to_string(),
        &format!("--effort={}", request.effort),
        &format!("--num_threads={}", request.threads.max(1)),
        "--quiet",
    ]);
    if request.effort == 11 {
        command.arg("--allow_expert_options");
    }
    if disable_reconstruction {
        command.args(["--allow_jpeg_reconstruction", "0"]);
    }
    command.arg(&request.source).arg(&request.destination);
}

fn pixel_signature(path: &Path) -> Result<String> {
    let output = StdCommand::new(image_tool())
        .arg(path)
        .args([
            "-alpha",
            "on",
            "-colorspace",
            "sRGB",
            "-depth",
            "16",
            "-format",
            "%w %h rgba %#",
            "info:",
        ])
        .output()?;
    ensure!(
        output.status.success(),
        "could not decode {}",
        path.display()
    );
    Ok(String::from_utf8(output.stdout)?.trim().into())
}

fn which(command: &str) -> bool {
    StdCommand::new(command).arg("--version").output().is_ok()
}

fn image_tool() -> &'static str {
    static TOOL: LazyLock<&'static str> =
        LazyLock::new(|| if which("magick") { "magick" } else { "convert" });
    *TOOL
}

async fn async_output(mut command: Command, timeout: Duration) -> Result<std::process::Output> {
    command.kill_on_drop(true);
    if timeout.is_zero() {
        return Ok(command.output().await?);
    }
    tokio::time::timeout(timeout, command.output())
        .await
        .context("media command timed out")?
        .map_err(Into::into)
}

fn parse_version(value: &str) -> Option<(u32, u32, u32)> {
    value
        .split(|character: char| !character.is_ascii_digit() && character != '.')
        .filter(|part| part.matches('.').count() == 2)
        .find_map(|part| {
            let mut values = part.split('.');
            Some((
                values.next()?.parse().ok()?,
                values.next()?.parse().ok()?,
                values.next()?.parse().ok()?,
            ))
        })
}

fn runtime_probe() -> Result<()> {
    let root = tempfile::tempdir()?;
    for extension in ["jpg", "png", "tiff", "bmp"] {
        let source = root.path().join(format!("source.{extension}"));
        checked(
            StdCommand::new(image_tool())
                .args(["-size", "16x16", "gradient:"])
                .arg(&source),
            "source image generation",
        )?;
        let input = if matches!(extension, "jpg" | "png") {
            source.clone()
        } else {
            let normalized = root.path().join(format!("normalized-{extension}.png"));
            checked(
                StdCommand::new(image_tool())
                    .arg(&source)
                    .args(["-auto-orient", "-colorspace", "sRGB", "-strip"])
                    .arg(&normalized),
                "source normalization",
            )?;
            normalized
        };
        let candidate = root.path().join(format!("candidate-{extension}.jxl"));
        checked(
            StdCommand::new("cjxl")
                .args(["-d", "0", "--effort=1", "--num_threads=1", "--quiet"])
                .arg(&input)
                .arg(&candidate),
            "lossless JPEG XL encoding",
        )?;
        let decoded = root.path().join(format!(
            "decoded-{extension}.{}",
            if extension == "jpg" { "jpg" } else { "png" }
        ));
        checked(
            StdCommand::new("djxl")
                .arg(&candidate)
                .arg(&decoded)
                .args(["--num_threads=1", "--quiet"]),
            "JPEG XL decoding",
        )?;
        ensure!(
            pixel_signature(&source)? == pixel_signature(&decoded)?,
            "lossless {extension} runtime probe changed pixels"
        );
    }
    Ok(())
}

fn checked(command: &mut StdCommand, label: &str) -> Result<()> {
    let output = command
        .output()
        .with_context(|| format!("{label} could not start"))?;
    ensure!(
        output.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_libjxl_versions() {
        assert_eq!(parse_version("cjxl v0.12.0 [AVX2]"), Some((0, 12, 0)));
        assert_eq!(parse_version("JPEG XL decoder v1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_version("unknown"), None);
    }
}
