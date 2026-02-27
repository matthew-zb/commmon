use commmon_rx_sdk::CommmonRxClient;

// commmon 데몬 실시간 RX 수신 예제
// 실행: cargo run --example monitor
// 사전 조건: commmon daemon 실행 중, COM 포트가 열려 있어야 함

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CommmonRxClient::connect("127.0.0.1", 9900).await?;
    println!("데몬 접속 완료");

    let mut rx = client.on_data();
    client.subscribe("COM14").await?;
    println!("COM14 구독 시작. Ctrl+C로 종료합니다.");

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(data) => println!("[{}] [{}] {}", data.timestamp, data.port, data.ascii),
                    Err(e) => {
                        eprintln!("수신 오류: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n종료 중...");
                break;
            }
        }
    }

    client.unsubscribe("COM14").await?;
    println!("종료");
    Ok(())
}
