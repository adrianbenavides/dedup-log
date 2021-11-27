use hashbrown::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use config::Config as CConfig;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

const TERMINATE_SEQUENCE: &str = "terminate";
const CONFIG_FILE: &str = "config.toml";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::new(CONFIG_FILE).context("Error loading config")?;
    tracing_subscriber::fmt::init();

    // Create an mpsc channel to listen for the exit signal. This signal can come from either
    // a TCP message containing the terminate sequence or the tokio's ctrlc signal, that's why
    // we need an mpsc and not a oneshot channel. We expect to receive at most 2 exit messages
    // at the same time so we can limit the channel buffer to that number, as we will exit the
    // program once the first exit message is processed.
    let (exit_tx, mut exit_rx) = mpsc::channel(2);

    // We pass the exit tx end to the DedupLog so it can send the exit signal back upon
    // receiving an exit message.
    let handle = { DedupLog::start(config.clone(), exit_tx).await? };

    // To limit the number of concurrent clients connected to the TCP listener, we use a Semaphore.
    // Each time a new client is accepted, it borrows permit until it's finished. Then the client
    // returns the permit back to the semaphore.
    let semaphore = Arc::new(Semaphore::new(config.max_clients));

    let tcp_listener = TcpListener::bind(format!("localhost:{}", config.port)).await?;
    tracing::debug!("Accepting connections at localhost:{}", config.port);
    loop {
        // First off, we borrow a permit from the semaphore. We need to do this first because
        // otherwise we won't let any change to the exit_rx to receive the message, as the
        // semaphore future will always end sooner.
        let permit = semaphore.clone().acquire_owned().await?;

        // In each loop turn, we wait for either the exit signal or a new client connection.
        tokio::select! {
            // If we receive the exit signal, we break the loop and exit the program.
            _ = exit_rx.recv() => {
                tracing::info!("Exit message received, closing TcpListener");
                break;
            },
            // If a new client is connected, we spawn a new task and handle the client's stream.
            res = tcp_listener.accept() => {
                let (stream, _) = res?;
                tracing::info!(
                    "Client accepted, remaining permits [permits={}]",
                    semaphore.available_permits()
                );
                let handle = handle.clone();
                // We move the permit to the task so it can drop it once it's done processing the stream.
                tokio::spawn(async move {
                    let _ = handle.stream(stream).await;
                    drop(permit);
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
struct Config {
    log_level: String,
    port: u32,
    max_clients: usize,
    flush_period: Duration,
    log_file: String,
}

impl Config {
    fn new(path: &str) -> anyhow::Result<Self> {
        let mut c = CConfig::new();
        c.merge(config::File::with_name(path))?;
        let config: Self = c.try_into()?;
        std::env::set_var("RUST_LOG", &config.log_level);
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: "debug".to_string(),
            port: 4000,
            max_clients: num_cpus::get(),
            flush_period: Duration::from_secs(10),
            log_file: "numbers.log".to_string(),
        }
    }
}

// This is the core struct. It's the only point accessing the in-memory log, and the one writing to the log file.
// A single instance of this struct will be running on a separate task in an infinite loop, listening for events to
// modify the vector and processing them sequentially. This way, we avoid the usage of synchronization primitives.
struct DedupLog {
    config: Config,
    // We want a data structure that supports fast inserts, as we only want to read on each flush_period.
    // We'll use a vector with the drawback that we'll have to delay the log writing operations and make
    // them in batch when printing the status report. Regarding the type, we use u32 instead of String to reduce memory usage.
    // Another obvious but important here is that we must store in memory the whole log to calculate the unique/duplicated items,
    // otherwise, we would have to read the log from disk and load it it memory anyway.
    log: Vec<u32>,
    // The counter of the unique items processed in the last period.
    // We'll use this to compute the duplicated/unique inserted items of the latest period.
    previous_period_len: usize,
}

impl DedupLog {
    async fn start(config: Config, exit_tx: mpsc::Sender<()>) -> anyhow::Result<DedupLogHandle> {
        tracing::debug!("Starting DedupLog");
        // Create or truncate the log file.
        File::create(&config.log_file).await?;
        // We expect to consume around 500k messages per second, so we initialize the vector size to 2^24
        let capacity = 16_777_216;
        // Create a channel to manage the events that this struct will listen to.
        let (event_tx, event_rx) = mpsc::channel(capacity);
        // Create a handle, which we'll use to send messages to this struct.
        let handle = DedupLogHandle::start(config.flush_period, event_tx, exit_tx).await;
        let dlog = Self {
            config,
            log: Vec::with_capacity(capacity),
            previous_period_len: 0,
        };
        // Move the instance to a separate task and start listening and return the handle.
        tokio::spawn(dlog.listen(event_rx));
        tracing::info!("DedupLog started");
        Ok(handle)
    }

    async fn listen(mut self, mut event_rx: mpsc::Receiver<Event>) -> anyhow::Result<()> {
        while let Some(event) = event_rx.recv().await {
            tracing::debug!("Event received: {:?}", event);
            match event {
                Event::Process(log_line) => self.process(log_line).await,
                Event::Status() => {
                    self.status().await?;
                    Ok(())
                }
                Event::Exit(exit_tx) => self.exit(&mut event_rx, exit_tx).await,
            }?;
        }
        Ok(())
    }

    async fn process(&mut self, log_line: LogLine) -> anyhow::Result<()> {
        // O(1) insert, move on. We'll handle duplicates in the status method.
        self.log.push(log_line.as_int);
        Ok(())
    }

    // Here is where the most heavy task is performed. Basically, we create a set out of the vec
    // to detect and remove the duplicates, write to the log file and print the status report.
    async fn status(&mut self) -> anyhow::Result<StatusReport> {
        // We get the elements added since the last period.
        let period = &self.log[self.previous_period_len..];
        let period_len = period.len();

        let _write_new_unique_to_log = {
            // We get the elements up to the last period (total = last_period + period).
            // We convert both slices to sets so we can compute the unique items of the current period
            // by computing the difference between them.
            let last_period = self.log[..self.previous_period_len]
                .iter()
                .collect::<HashSet<_>>();
            let period_unique = period.iter().collect::<HashSet<_>>();
            let to_write = period_unique.difference(&last_period).collect::<Vec<_>>();

            // We can now write the new unique items to the log file.
            let mut log_file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(&self.config.log_file)
                .await?;

            // To reduce the I/O overhead, we create a String with all the contents we want
            // to dump so we write to the file just once.
            let mut buffer = String::new();

            // If there are more than 1_000_000 items to write, we write them in chunks to avoid
            // allocating too much memory at once at the expense of doing multiple I/O operations.
            for chunk in to_write.chunks(1_000_000) {
                buffer.clear();
                for item in chunk {
                    buffer.push_str(&format!("{}\n", LogLine::try_from(***item)?.line));
                }
                log_file.write_all(buffer.as_bytes()).await?;
            }
        };

        // We compute now the rest of the metrics for the status report.
        // By draining the vector we both empty the vector and move the values
        // into the new structure, avoiding cloning them.
        let total_len = self.log.len();
        let total_unique: HashSet<u32> = self.log.drain(..).collect();
        let status = {
            let total_unique_len = total_unique.len();
            let period_duplicated_len = total_len - total_unique_len;
            let period_unique_len = period_len - period_duplicated_len;
            let status =
                StatusReport::new(period_unique_len, period_duplicated_len, total_unique_len);
            tracing::info!("{}", status);
            status
        };

        // We convert the total_unique set back to a vec, consuming the set to avoid cloning the values.
        let _update_log = {
            self.log = total_unique.into_iter().collect();
            self.previous_period_len = self.log.len();
        };

        Ok(status)
    }

    async fn exit(
        &mut self,
        event_rx: &mut mpsc::Receiver<Event>,
        exit_tx: mpsc::Sender<()>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Waiting event channel to drain messages");
        // Closing a channel waits until all buffered messages are processed.
        event_rx.close();
        // After the channel is closed, we run the status method one last time.
        self.status().await?;
        tracing::debug!("Sending exit message to exit channel");
        // Best effort trying to send the exit message back to the exit channel.
        let _ = exit_tx.send(()).await;
        Ok(())
    }
}

struct StatusReport {
    period_unique_len: usize,
    period_duplicated_len: usize,
    total_unique_len: usize,
}

impl StatusReport {
    fn new(
        period_unique_len: usize,
        period_duplicated_len: usize,
        total_unique_len: usize,
    ) -> Self {
        Self {
            period_unique_len,
            period_duplicated_len,
            total_unique_len,
        }
    }
}

impl std::fmt::Display for StatusReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Received {} unique numbers, {} duplicates. Unique total: {}",
            self.period_unique_len, self.period_duplicated_len, self.total_unique_len
        )
    }
}

// This struct only holds the transmitter ends of the channels used in this program: one to handle
// the exit signal, and another to handle the messages. As it only has the Sender sides of channels,
// it can be cloned cheaply.
#[derive(Clone)]
struct DedupLogHandle {
    event_tx: mpsc::Sender<Event>,
    exit_tx: mpsc::Sender<()>,
}

impl DedupLogHandle {
    async fn start(
        flush_period: Duration,
        event_tx: mpsc::Sender<Event>,
        exit_tx: mpsc::Sender<()>,
    ) -> Self {
        tracing::debug!("Starting DedupLogHandle");
        let handle = Self { event_tx, exit_tx };
        let _exit_signal = {
            // Create a new task to listen to the tokio's ctrlc signal.
            let handle = handle.clone();
            tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(_) => {
                        tracing::debug!("Tokio's ctrl_c signal received");
                        // Best effort to try sending the message to the channel. Whether it succeeds
                        // or not, we drop this task, so we don't care about the result.
                        let _ = handle.close().await;
                    }
                    Err(err) => {
                        // We could end up here if the OS fails to register the signal.
                        // In that case, we won't be able to react to this signal, so
                        // we print the error and we move on.
                        tracing::debug!("{}", err);
                    }
                }
            });
        };
        let _status_signal = {
            // Create a new task to send periodical Status messages to the DedupLog instance.
            // This must be handled in an exclusive loop to have full control over its
            // iterations and avoid interrupting the sleep future.
            // If the system is loaded with a lot of events, the processing of this event could be delayed.
            let handle = handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(flush_period).await;
                    // If this fails, the channel is closed, so we break the loop and drop this task.
                    if handle.status().await.is_err() {
                        break;
                    }
                }
            });
        };
        tracing::info!("DedupLogHandle started");
        handle
    }

    // The main method of this struct. Transforms the TcpStream instance into an object that can
    // be easily iterated as a file, one line at a time as they arrive from the client.
    async fn stream(self, stream: TcpStream) -> anyhow::Result<()> {
        let mut lines = Framed::new(stream, LinesCodec::new());
        while let Some(Ok(line)) = lines.next().await {
            // If there is any error processing the message we want to stop iterating the stream
            // so we can close it and finish with this client.
            if self.process_message(line).await.is_err() {
                break;
            }
        }
        tracing::info!("Client finished");
        Ok(())
    }

    async fn process_message(&self, line: String) -> anyhow::Result<()> {
        let event = Event::new(line, self.exit_tx.clone()).await?;
        self.event_tx.send(event).await?;
        Ok(())
    }

    async fn status(&self) -> anyhow::Result<()> {
        tracing::debug!("Sending Status signal");
        self.event_tx.send(Event::Status()).await?;
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        tracing::debug!("Sending Exit signal");
        self.event_tx
            .send(Event::Exit(self.exit_tx.clone()))
            .await?;
        tracing::debug!("DedupLogHandle was closed");
        Ok(())
    }
}

#[derive(Debug)]
enum Event {
    Process(LogLine),
    Status(),
    Exit(mpsc::Sender<()>),
}

impl Event {
    async fn new(line: String, exit_tx: mpsc::Sender<()>) -> anyhow::Result<Self> {
        Ok(match line.as_str() {
            TERMINATE_SEQUENCE => Event::Exit(exit_tx),
            _ => Event::Process(LogLine::try_from(line)?),
        })
    }
}

#[derive(Debug)]
struct LogLine {
    line: String,
    as_int: u32,
}

impl LogLine {
    const LINE_LENGTH: usize = 9;
}

impl TryFrom<String> for LogLine {
    type Error = anyhow::Error;

    fn try_from(line: String) -> Result<Self, Self::Error> {
        Self::try_from(line.as_str())
    }
}

impl TryFrom<&str> for LogLine {
    type Error = anyhow::Error;

    fn try_from(line: &str) -> Result<Self, Self::Error> {
        if line.len() != Self::LINE_LENGTH {
            return Err(anyhow!("Line length must be exactly {}", Self::LINE_LENGTH));
        }
        let line = line.to_string();
        let as_int = line.parse::<u32>()?;
        Ok(Self { line, as_int })
    }
}

impl TryFrom<u32> for LogLine {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let as_string = format!("{}", value);
        if as_string.len() > Self::LINE_LENGTH {
            return Err(anyhow!(
                "Number must have less than {} digits",
                Self::LINE_LENGTH
            ));
        }
        Ok(Self {
            line: format!("{:0>length$}", value, length = Self::LINE_LENGTH),
            as_int: value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn handle_messages() {
        let config = Config::default();
        File::create(&config.log_file).await.unwrap();

        let mut sut = DedupLog {
            config,
            log: vec![],
            previous_period_len: 0,
        };

        sut.process(LogLine::try_from(1).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(2).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(3).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(2).unwrap()).await.unwrap();
        assert_eq_unordered_slice(&sut.log, &[1, 2, 3, 2]);

        let status = sut.status().await.unwrap();
        assert_eq!(status.period_duplicated_len, 1);
        assert_eq!(status.period_unique_len, 3);
        assert_eq!(status.total_unique_len, 3);
        assert_eq_unordered_slice(&sut.log, &[1, 2, 3]);

        sut.process(LogLine::try_from(4).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(3).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(5).unwrap()).await.unwrap();
        sut.process(LogLine::try_from(5).unwrap()).await.unwrap();
        assert_eq_unordered_slice(&sut.log, &[1, 2, 3, 4, 3, 5, 5]);

        let status = sut.status().await.unwrap();
        // Note that in this case, the 5 is both unique and duplicated.
        assert_eq!(status.period_duplicated_len, 2); // So we have here the 3 and the 5.
        assert_eq!(status.period_unique_len, 2); // And the 4 and the 5 here.
        assert_eq!(status.total_unique_len, 5);
        assert_eq_unordered_slice(&sut.log, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn log_line_validations() {
        assert!(LogLine::try_from(4_294_967_295).is_err());
        let sut = LogLine::try_from(999_999_999).unwrap();
        assert_eq!(&sut.line, "999999999");
        assert_eq!(sut.as_int, 999_999_999);

        assert!(LogLine::try_from("4294967295").is_err());
        assert!(LogLine::try_from("001").is_err());
        let sut = LogLine::try_from("001283467").unwrap();
        assert_eq!(&sut.line, "001283467");
        assert_eq!(sut.as_int, 1283467);
    }

    pub fn assert_eq_unordered_slice<T: Eq + PartialEq>(va: &[T], vb: &[T]) {
        assert_eq!(va.len(), vb.len());
        for x in va.iter() {
            assert!(vb.contains(x));
        }
    }
}
