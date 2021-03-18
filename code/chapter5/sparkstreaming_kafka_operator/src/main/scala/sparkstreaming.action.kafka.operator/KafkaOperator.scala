package sparkstreaming.action.kafka.operator

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/**
 * 消费 kafka 数据进行 join
 */
object KafkaOperator extends App {
  private val conf: SparkConf = new SparkConf().setMaster("local[1]")
    .setAppName("KafkaOperator").set("spark.streaming.kafka.maxRatePerPartition", "10")
  private val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  private val ctx = new StreamingContext(sc,Seconds(2))

  // 连接kafka
  private val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "KafkaOperator",
    "enable.auto.commit" -> (false: lang.Boolean),
    "auto.offset.reset" -> "latest"
  )
  // 从 kafkaOperation 分区读数据
  private val kafkaDirectStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ctx,
    PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List("kafkaOperation"), kafkaParams)
  )

  // 过滤出 userPhone, map 成(name, phone) 数据格式"nick0/t144000334264320/t1"
  private val userPhoneStream: DStream[(String, String)] = kafkaDirectStream.map(_.value())
    .filter(_.endsWith("\t1")).map(userPhone => {
    val tokens = userPhone.split("\t")
    (tokens(0), tokens(1))
  }
  )

  //  过滤出 userAddress, map 成(user, address) 数据格式"nick0/tshanghai#2000004252/t0"
  private val userAddressStream: DStream[(String, String)] = kafkaDirectStream.map(_.value())
    .filter(_.endsWith("\t0")).map(userPhone => {
    val tokens = userPhone.split("\t")
    (tokens(0), tokens(1))
  }
  )

  // join 出user address 和 phone
  private val userPhoneAddrStream: DStream[String] = userPhoneStream.join(userAddressStream).map(joinedRecord => {
    s"name:${joinedRecord._1}, address:${joinedRecord._2._1}, phone:${joinedRecord._2._2}"
  }
  )
  //  private val userPhoneAddrStream: DStream[(String, String)] = userAddressStream

  userPhoneAddrStream.print
  ctx.start()
  ctx.awaitTermination()
}

