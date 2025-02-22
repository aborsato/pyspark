# pyspark

# Running
## Kafka
```bash
# Start Kafka broker
docker compose up

# Connect to the container
docker exec -it -w /opt/kafka/bin broker sh
# From inside the container
./kafka-topics.sh --create --topic topic1 --bootstrap-server broker:29092
./kafka-console-producer.sh  --topic topic1 --bootstrap-server broker:29092

```
Run other command inside the container [here](https://developer.confluent.io/confluent-tutorials/kafka-on-docker/)

## Spark Streaming
```bash
spark-submit --properties-file kafka-streaming.properties kafka-streaming.py
```
