package sparkstreaming.action.producer.main

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.concurrent.Executors
import scala.collection.JavaConversions._
import java.util.{Collections, Properties}

class ConsumerTest(brokers: String, topic: String, groupId: String) extends Logging{
  private val props: Properties = getConsumerProps(brokers, groupId)
  private val consumer = new KafkaConsumer[String, String](props)

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)
          println("consumer.poll" + records.size)
          for (record <- records) {
            println(s"consumer a record, (k, v): (${record.key()}, ${record.value()}), offset: ${record.offset()}")
          }
        }
      }
    })
  }

  def shutdown() =  {
    if (consumer != null) {
      consumer.close()
    }
  }

  def getConsumerProps(brokers: String, groupId: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props
  }
}

object ConsumerTest extends App {
  private val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  private val topic = "behaviorGeneratorTest"
  private val test = new ConsumerTest(brokers, topic, "ConsumerTest")
  test.run()
}
