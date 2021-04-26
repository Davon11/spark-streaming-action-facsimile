package sparkstreaming.action.producer.main

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object BehaviorProducer extends App {
  private val events = 100
  private val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  private val topic = "behaviorGeneratorTest"

  private val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)
  private val start: Long = System.currentTimeMillis()

  private val rnd = new Random()
  for (i <- Range(0, events)) {
    val user = rnd.nextInt(events)
    val item = rnd.nextInt(events)
    val ts = System.currentTimeMillis()
    val value = s"${ts}\t${user}\t${item}"
    val record = new ProducerRecord[String, String](topic, user.toString, value)
    producer.send(record)

    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }

  println("sent record per second:" + (events * 1000 / (System.currentTimeMillis() - start)))
  producer.close()
}
