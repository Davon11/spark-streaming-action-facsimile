package sparkstreaming.log.analysis

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties

object CommentGenerator extends App {
  private val bootstrap = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  private val clientID = "CommentGenerator"
  private val props = new Properties()

  props.put("bootstrap.servers", bootstrap);
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("client.id", clientID)

  private val producer = new KafkaProducer(props)
}
