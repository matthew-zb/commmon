mod daemon;
mod monitor;
mod protocol;
mod repl;
mod serial;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "commmon", about = "COM Port 시리얼 통신 도구")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// 데몬 TCP 포트 (REPL 모드에서 사용)
    #[arg(short, long, default_value_t = 9900)]
    port: u16,
}

#[derive(Subcommand)]
enum Commands {
    /// 데몬 모드 (TCP 서버 + 시리얼 포트 관리)
    Daemon {
        /// TCP 서버 포트 (기본: 9900)
        #[arg(short, long, default_value_t = 9900)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Daemon { port }) => {
            daemon::run(port).await?;
        }
        None => {
            repl::run(cli.port).await?;
        }
    }

    Ok(())
}
