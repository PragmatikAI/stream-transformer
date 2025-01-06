from confluent_kafka import Producer, Consumer, KafkaError
import snowplow_analytics_sdk.event_transformer
import snowplow_analytics_sdk.snowplow_event_transformation_exception
import json
import logging
import utils
from pyhocon import ConfigFactory

logging.basicConfig(level=logging.INFO)

def run(bootstrap_server, group_id, enriched_topic, transformed_topic):
  try:
    logging.info(f"Starting Kafka Transformer with \n- bootstrap server: {bootstrap_server} \n- group id: {group_id} \n- enriched topic: {enriched_topic} \n- transformed topic: {transformed_topic}")
    kafka_consumer = Consumer({
      'bootstrap.servers': bootstrap_server,
      'group.id': group_id,
      'default.topic.config': {
        'auto.offset.reset': 'latest'
      }
    })

    kafka_producer = Producer({
      'bootstrap.servers': bootstrap_server,
    })

    kafka_consumer.subscribe([enriched_topic])

    while True:
      logging.debug("Polling for messages")
      msg = kafka_consumer.poll(1.0)

      if msg is None:
        continue
      if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF: continue
        else:
          logging.error(msg.error())
          break

      event = msg.value().decode('utf-8')

      try:
        json_data = snowplow_analytics_sdk.event_transformer.transform(event)
        json_data = utils.flatten_event(json_data)
        kafka_producer.poll(0)
        kafka_producer.produce(transformed_topic, json.dumps(json_data).encode('utf-8'))
        kafka_producer.flush()

      except snowplow_analytics_sdk.snowplow_event_transformation_exception.SnowplowEventTransformationException as e:
        for error_message in e.error_messages:
          logging.error("error: " + error_message)

    kafka_consumer.close()
  except Exception as e:
    logging.error(e)
    logging.error("Error in Kafka Transformer")


if __name__ == "__main__":
  conf = ConfigFactory.parse_file('/pragmatik/config/transformer-config.hocon')

  if conf.get("logging.level") == "DEBUG":
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  run(conf.get("input.bootstrapServers"), conf.get("input.consumerConf.group_id"), conf.get("input.topicName"), conf.get("output.topicName"))
