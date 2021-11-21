use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut handles = Vec::with_capacity(5);
    for _ in 0..10 {
        let handle = tokio::spawn(async {
            let mut stream = TcpStream::connect("localhost:4000").await.unwrap();
            for _ in 0..1_000_000 {
                let message = {
                    let mut rng = rand::thread_rng();
                    let value: u32 = rng.gen_range(0..999_999);
                    format!("{:0>9}\n", value)
                };
                let _ = stream.write_all(message.as_bytes()).await;
                tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.await?;
    }
    let mut stream = TcpStream::connect("localhost:4000").await.unwrap();
    let _ = stream.write_all(b"terminate\n").await;
    Ok(())
}
