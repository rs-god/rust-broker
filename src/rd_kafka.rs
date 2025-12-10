use crate::broker::Broker;
use crate::kafka_config::KafkaConfig;
use async_trait::async_trait;
use futures::StreamExt;
use log::{error, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use std::time::Duration;
use tokio::sync::broadcast;

pub struct KafkaImpl {
    config: KafkaConfig,
    producer: FutureProducer,
    shutdown_tx: broadcast::Sender<()>,
}

// new broker instance
pub fn new_broker(config: KafkaConfig) -> Result<impl Broker, String> {
    KafkaImpl::new(config)
}

// impl kafka broker
#[async_trait]
impl Broker for KafkaImpl {
    async fn publish(&self, topic: &str, msg: &[u8]) -> Result<(), String> {
        let record = FutureRecord::to(topic).payload(msg).key(&());
        match self
            .producer
            .send(record, self.config.message_timeout)
            .await
        {
            Ok(_) => {
                info!("message sent to topic: {} success", topic);
                Ok(())
            }
            Err((err, _)) => {
                let err_msg = format!("failed to publish message to topic:{} error:{}", topic, err);
                error!("{}", err_msg);
                Err(err_msg)
            }
        }
    }

    async fn subscribe<F, Fut>(&self, topic: &str, group: &str, handler: F) -> Result<(), String>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let consumer = Self::create_consumer(&self.config, group)?;
        // sub topic
        consumer
            .subscribe(&[topic])
            .map_err(|e| format!("failed to subscribe to topic:{} error:{}", topic, e))?;

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        info!("subscribe to topic: {} group:{} begin...", topic, group);

        // create consumer stream
        let mut stream = consumer.stream();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("received shutdown signal");
                    break;
                }

                message_result = stream.next() => {
                    match message_result {
                        Some(Ok(message)) => {
                            // received message
                            if let Some(payload) = message.payload() {
                                let msg = payload.to_vec();
                                match handler(msg).await {
                                    Ok(_) => {
                                        // ack message to kafka
                                        if !self.config.enable_auto_commit {
                                            if let Err(err) = consumer.commit_message(&message, CommitMode::Async) {
                                                error!(
                                                    "failed to commit message to consumer topic:{} error:{}",
                                                    topic, err
                                                );
                                            } else {
                                                info!(
                                                    "commit message ack for topic:{} group:{} success",
                                                    topic,group
                                                );
                                            }
                                        } else {
                                            info!(
                                                "consume message from topic:{} group:{} success",
                                                topic,group
                                            );
                                        }
                                    },
                                    Err(err) => {
                                        error!(
                                            "failed to handler message from topic:{} group:{} error:{}",
                                            topic, group,err
                                        );
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(
                                "failed to read message from topic:{} group:{} error:{}",
                                topic, group,e
                            );
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        },
                        None => {
                            // Other unknown reasons for the exit
                            // might be network abnormalities or other causes.
                            warn!(
                                "consumer stream topic:{} group:{} will stop with other unknown reason",
                                topic, group
                            );
                            break;
                        }
                    }
                }
            }
        }

        info!("consumer stream topic:{} group:{} stopped", topic, group);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), String> {
        let sub_count = self.shutdown_tx.receiver_count();
        if sub_count > 0 {
            info!("send shutdown signal to {} consumers", sub_count);
            let _ = self.shutdown_tx.send(());

            info!(
                "waiting {:?} consumers to stop",
                self.config.graceful_wait_timeout
            );
            tokio::time::sleep(self.config.graceful_wait_timeout).await;
        }

        info!("flushing producer...");
        tokio::time::sleep(Duration::from_secs(1)).await;
        info!("kafka broker stopped");
        Ok(())
    }
}

impl KafkaImpl {
    pub fn new(config: KafkaConfig) -> Result<Self, String> {
        let producer = Self::create_producer(&config)?;
        let (shutdown_tx, _) = broadcast::channel(1);
        Ok(Self {
            config,
            producer,
            shutdown_tx,
        })
    }

    fn create_producer(config: &KafkaConfig) -> Result<FutureProducer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set(
                "message.timeout.ms",
                config.message_timeout.as_millis().to_string(),
            )
            .set(
                "message.send.max.retries",
                config.message_send_max_retries.to_string(),
            )
            .set("message.max.bytes", config.message_max_bytes.to_string());

        Self::config_security(&mut client_config, config)?;

        client_config
            .create()
            .map_err(|e| format!("failed to create kafka producer: {}", e))
    }

    fn create_consumer(config: &KafkaConfig, group_id: &str) -> Result<StreamConsumer, String> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("session.timeout.ms", "30000")
            .set("heartbeat.interval.ms", "3000");

        if config.enable_auto_commit {
            client_config.set("enable.auto.commit", "true");
        } else {
            client_config
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false");
        }

        Self::config_security(&mut client_config, config)?;

        client_config
            .create()
            .map_err(|e| format!("failed to create kafka consumer: {}", e))
    }

    // security settings
    fn config_security(
        client_config: &mut ClientConfig,
        config: &KafkaConfig,
    ) -> Result<(), String> {
        let protocol = config.security_protocol.to_uppercase();
        match protocol.as_str() {
            "PLAINTEXT" => {
                client_config.set("security.protocol", "PLAINTEXT");
            }
            "SASL_PLAINTEXT" => {
                let sasl_mechanism = config.sasl_mechanism.to_uppercase();
                client_config
                    .set("security.protocol", "SASL_PLAINTEXT")
                    .set("sasl.mechanism", &sasl_mechanism);

                // set username/password
                if sasl_mechanism == "PLAIN"
                    || sasl_mechanism.starts_with("SCRAM-SHA")
                    || config
                        .sasl_type_scram_sha
                        .to_uppercase()
                        .starts_with("SCRAM-SHA")
                {
                    client_config
                        .set("sasl.username", &config.username)
                        .set("sasl.password", &config.password);
                }

                info!(
                    "using SASL_PLAINTEXT protocol with mechanism:{}",
                    &config.sasl_mechanism
                );
            }
            "SASL_SSL" => {
                let sasl_mechanism = config.sasl_mechanism.to_uppercase();
                client_config
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanism", &sasl_mechanism);

                // set username/password
                if sasl_mechanism == "PLAIN"
                    || sasl_mechanism.starts_with("SCRAM-SHA")
                    || config
                        .sasl_type_scram_sha
                        .to_uppercase()
                        .starts_with("SCRAM-SHA")
                {
                    client_config
                        .set("sasl.username", &config.username)
                        .set("sasl.password", &config.password);
                }

                // add kafka ca-cert.pem
                if !config.cert_path.is_empty() {
                    client_config.set("ssl.ca.location", &config.cert_path);
                }

                // Whether to skip certificate verification
                let enable_verify = if config.insecure_skip_verify {
                    false
                } else {
                    true
                };
                client_config.set(
                    "enable.ssl.certificate.verification",
                    enable_verify.to_string(),
                );

                info!(
                    "using SASL_SSL protocol with mechanism:{} insecure_skip_verify:{}",
                    &config.sasl_mechanism, config.insecure_skip_verify
                );
            }
            _ => {
                return Err(format!(
                    "unsupported protocol,unknown protocol: {}",
                    protocol
                ));
            }
        }

        Ok(())
    }
}
