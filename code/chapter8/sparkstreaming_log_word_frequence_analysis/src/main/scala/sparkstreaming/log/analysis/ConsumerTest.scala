package sparkstreaming.log.analysis


import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.util.concurrent.{Executor, Executors}
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._

class ConsumerTest(val brokers: String,
                   val groupID: String,
                   val topic: String) {

  private val props: Properties = createConsumerConfig(brokers, groupID)
  private val consumer = new KafkaConsumer[String, String](props)

  def run() = {
    consumer.subscribe(Collections.singletonList(topic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)
          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }

  def consumerShutDown() = {
    if (this.consumer != null)
      consumer.close()
  }

  def createConsumerConfig(brokers: String, groupID: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }
}

object ConsumerTest extends App {
  private val consumer = new ConsumerTest("hadoop102:9092,hadoop103:9092,hadoop104:9092", "ConsumerTest", "CommentLog")
  consumer.run()

}
