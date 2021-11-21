use rand::seq::SliceRandom;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();
    let mut stream = TcpStream::connect("localhost:4000").await.unwrap();
    for _ in 0..100_000 {
        let message = {
            let value: u32 = rng.gen_range(0..999_999);
            let invalid_value = format!("X{:0>8}\n", value);
            let valid_value = format!("{:0>9}\n", value);
            vec![invalid_value, valid_value]
                .choose(&mut rng)
                .unwrap()
                .clone()
        };
        let _ = stream.write_all(message.as_bytes()).await;
    }
    let _ = stream.write_all(b"terminate\n").await;
    Ok(())
}
