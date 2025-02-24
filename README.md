# pyspark

## References
- https://github.com/NotAndex/kafka_iot_sim
- https://github.com/hifly81/kafka-examples

## Running
### Kafka
```bash
# Start Kafka broker
docker compose up

# Connect to the container
docker exec -it -w /opt/kafka/bin broker sh
# From inside the container
./kafka-topics.sh --create --topic topic1 --bootstrap-server broker:29092
./kafka-topics.sh --create --topic topic2 --bootstrap-server broker:29092
./kafka-console-producer.sh  --topic topic1 --bootstrap-server broker:29092
./kafka-console-consumer.sh --topic topic2 --from-beginning --bootstrap-server broker:29092
```
Run other command inside the container [here](https://developer.confluent.io/confluent-tutorials/kafka-on-docker/)

### Spark Streaming
```bash
spark-submit --properties-file kafka-streaming.properties kafka-streaming.py
```
More info [here](https://spark.apache.org/docs/3.5.1/structured-streaming-kafka-integration.html#deploying)
