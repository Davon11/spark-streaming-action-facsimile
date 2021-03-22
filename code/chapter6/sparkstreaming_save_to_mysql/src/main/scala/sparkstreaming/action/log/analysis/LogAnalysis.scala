package sparkstreaming.action.log.analysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

case class Record(logLevel: String, method: String, context: String)
object LogAnalysis {
  def main(args: Array[String]): Unit = {
    // mySQL 配置
    val mysqlProp = new Properties()
    mysqlProp.setProperty("user", "root")
    mysqlProp.setProperty("password", "987654")
//    mysqlProp.setProperty("serverTimezone", "Asia/Shanghai")

    // 如果在多台机器上启动集群，则从本地读取文件时，要求每台机器上都有该文件。
    val conf = new SparkConf().setAppName("LogAnalysis").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(2))
    val path = "/opt/module/spark-standalone/spark_action/save_to_mysql"
//    val path = "D:\\04.Study\\02.StudyWorkSpace\\06.SparkStreaming\\spark-streaming-action-facsimile2\\code\\chapter6\\sparkstreaming_save_to_mysql\\src\\main\\resources"
    val inputPath = path + "/input"
    val inputStream = ssc.textFileStream(inputPath)
    inputStream.foreachRDD{ rdd =>
      import spark.implicits._
      val data = rdd.map { line =>
        val tokens = line.split("\t")
        Record(tokens(0), tokens(1), tokens(2))
      }.toDF()

      data.createOrReplaceTempView("all_data")
      val errorLog: DataFrame = spark.sql("select * from all_data where logLevel = '[error]' or logLevel = '[warn]'")
      errorLog.show(10)

      // 输出到外部mysql中
      val mysqlHost = "192.168.31.93"
//      val schema = StructType(Array(StructField("logLevel", StringType, true), StructField("method", StringType, true),
//        StructField("context", StringType, true)))
      errorLog.write.mode(SaveMode.Append)
        .jdbc(f"jdbc:mysql://$mysqlHost:3306/log_analysis?serverTimezone=Asia/Shanghai", "important_logs", mysqlProp)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
