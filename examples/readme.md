# start kafka in docker
```shell
docker-compose up -d
```

# publish message
```shell
cargo run --example publish_basic 
```

# subscribe message
```shell
cargo run --example sub_basic
```