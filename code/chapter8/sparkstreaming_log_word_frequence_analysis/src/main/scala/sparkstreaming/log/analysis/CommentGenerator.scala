package sparkstreaming.log.analysis

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.BufferedSource
import scala.util.Random

object CommentGenerator extends App {
  private val BOOTSTRAP = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  private val CLIENT_ID = "CommentGenerator"
  private val TOPIC = "CommentLog"
  private val events: Int = args(0).toInt
  private val rnd = new Random()

  private val props = new Properties()
  props.put("bootstrap.servers", BOOTSTRAP);
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("client.id", CLIENT_ID)
  private val producer = new KafkaProducer[String, String](props)

  private val dictFile = "D:\\04.Study\\02.StudyWorkSpace\\06.SparkStreaming\\spark-streaming-action-facsimile2\\code\\chapter8\\sparkstreaming_log_word_frequence_analysis\\src\\main\\resources\\hanzi.txt"
  private val source: BufferedSource = scala.io.Source.fromFile(dictFile)
  private val line: String = try source.mkString finally source.close()
  private val length: Int = line.length
  private val t: Long = System.currentTimeMillis()

  for (i <- Range(0, events)) {
    val builder = new StringBuilder()
    val commentLength = rnd.nextInt(180) + 20
    for (i <- Range(0, commentLength)) {
      builder.append(line.charAt(rnd.nextInt(length)))
    }
    val userName = "user_" + rnd.nextInt(200)

    producer.send(new ProducerRecord[String, String](TOPIC, userName, builder.toString()))
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()

}
