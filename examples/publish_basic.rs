use rs_broker::Broker;
use rs_broker::{KafkaConfig, new_broker};
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // 初始化logger配置
    // 日志level 优先级  error > warn > info > debug > trace
    // 设置日志级别环境变量，这里注释掉了，启动的时可手动指定RUST_LOG=debug
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }

    env_logger::init();
    let brokers = env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string());

    // kafka config
    let kafka_config = KafkaConfig::builder(&brokers)
        .with_security_protocol("PLAINTEXT")
        .build();

    // create kafka broker
    let broker = Arc::new(new_broker(kafka_config).expect("Could not create Kafka broker"));
    let broker_clone = broker.clone();
    let topic = "my-topic";
    let handler = tokio::spawn(async move {
        for i in 1..10 {
            let delivery_status = broker_clone
                .publish(topic, format!("hello,world,{}", i).as_bytes())
                .await;
            if let Err(err) = delivery_status {
                println!("Error sending message: {:?}", err);
                continue;
            }

            println!("delivery status: {:?}", delivery_status);
        }
    });
    handler.await.unwrap();

    // shutdown broker
    let _ = broker.shutdown().await;
}
