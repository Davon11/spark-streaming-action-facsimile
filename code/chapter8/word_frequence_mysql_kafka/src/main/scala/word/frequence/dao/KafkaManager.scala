package word.frequence.dao

import scala.reflect.ClassTag
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

class KafkaManager(val kafkaParams: Map[String, String]){
  private val logger: Logger = LogManager.getLogger(this.getClass)
  new KafkaCluster(kafkaParams)


}

object KafkaManager {

}
