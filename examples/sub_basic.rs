use rs_broker::Broker;
use rs_broker::{KafkaConfig, new_broker};
use std::env;

#[tokio::main]
async fn main() {
    // 初始化logger配置
    // 日志level 优先级  error > warn > info > debug > trace
    // 设置日志级别环境变量，下面注释掉了，启动的时可手动指定RUST_LOG=debug
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
    let broker = new_broker(kafka_config).expect("Could not create Kafka broker");
    let topic = "my-topic";
    let group = "group-1";
    println!("consume message begin...");
    broker
        .subscribe(topic, group, |payload| {
            let message = String::from_utf8_lossy(&payload).to_string();
            async move {
                println!("handler msg");
                let _ = handler(&message).await;
                // return ok
                Ok(())
            }
        })
        .await
        .expect("Could not subscribe to topic");

    // 通过异步函数处理
    // broker
    //     .subscribe(topic, group, async_handler)
    //     .await
    //     .expect("Could not subscribe to topic");
}

async fn async_handler(msg: Vec<u8>) -> Result<(), String> {
    let s = String::from_utf8_lossy(&msg).to_string();
    println!("handler msg {}", s);
    let _ = handler(&s).await;
    Ok(())
}

async fn handler(msg: &str) -> Result<(), String> {
    println!("Got msg len: {}", msg.len());
    println!("consumer msg: {}", msg);
    Ok(())
}
