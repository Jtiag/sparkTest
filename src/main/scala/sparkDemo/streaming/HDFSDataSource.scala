package sparkDemo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiangtao7 on 2017/11/2.
  */
object HDFSDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("HDFSDataSource")

    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.textFileStream("hdfs://Amb:8020/wordcount_dir/")

    val words = lines.flatMap(_.split(" "))
    val wordsPairs = words.map((_,1))
    val wordCount = wordsPairs.reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
