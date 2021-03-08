package sparkstreaming.action.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object MyKafkaProducer extends App {
  private val KAFKA_TOPIC = "kafkaOperation"
  private val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  private val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "KafkaReducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // 建立kafka连接
  private val producer = new KafkaProducer[String, String](props)
  private val userAddrMap = Map("bob" -> "shanghai#200000", "amy" -> "beijing#100000", "alice" -> "shanghai#200000",
    "tom" -> "beijing#100000", "lulu" -> "hangzhou#310000", "nick" -> "shanghai#200000")
  private val userPhoneMap = Map("bob" -> "15700079421", "amy" -> "18700079458", "alice" -> "17730079427",
    "tom" -> "16700379451", "lulu" -> "18800074423", "nick" -> "14400033426")

  // 模拟数据并发送
  private val rdn: Random = new Random()
  private val t: Long = System.currentTimeMillis()
  private val target: Int = rdn.nextInt(10)
  var ts = 0L
  for (elem <- userAddrMap) {
    ts = System.currentTimeMillis() % 10000
    val recordStr = s"${elem._1}${target}\t${elem._2}${ts}\t0"
    producer.send(new ProducerRecord[String, String](KAFKA_TOPIC, elem._1, recordStr))
    if (rdn.nextInt(100) > 50) Thread.sleep(rdn.nextInt(10))
  }
  for (elem <- userPhoneMap) {
    ts = System.currentTimeMillis() % 10000
    val recordStr = s"${elem._1}${target}\t${elem._2}${ts}\t1"
    producer.send(new ProducerRecord[String, String](KAFKA_TOPIC, elem._1, recordStr))
    if (rdn.nextInt(100) > 50) Thread.sleep(rdn.nextInt(10))
  }
  System.out.println("send per second: " + (userPhoneMap.size + userAddrMap.size) * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
