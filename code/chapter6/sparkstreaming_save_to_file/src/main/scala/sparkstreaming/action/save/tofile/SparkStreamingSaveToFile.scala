package sparkstreaming.action.save.tofile

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingSaveToFile {
  def main(args: Array[String]): Unit = {
//    val patentPath = args(0)
    val patentPath = "D:\\04.Study\\02.StudyWorkSpace\\06.SparkStreaming\\spark-streaming-action-facsimile2\\code\\chapter6\\sparkstreaming_save_to_file\\src\\main\\resources"
    val inputPath = patentPath + "\\input"
    val outputPath = patentPath + "/output"
    print("inputPath: " + inputPath + "\noutputPath: " + outputPath)
    val conf = new SparkConf().setAppName("readfile from file local").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val inputStream: DStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordCountStream: DStream[(String, Int)] = inputStream.flatMap(line => line.split(" "))
      .map{ word =>
        (word, 1) }.reduceByKey(_+_)
    wordCountStream.print()
    wordCountStream.saveAsTextFiles("file:///" + outputPath + "/saveAsTextFiles")
    wordCountStream.saveAsObjectFiles("file:///" + outputPath + "/saveAsObjectFiles")

    ssc.start()
    ssc.awaitTermination()
  }

}
