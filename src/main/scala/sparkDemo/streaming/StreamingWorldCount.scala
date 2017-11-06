package sparkDemo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiangtao7 on 2017/11/2.
  */
object StreamingWorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWorldCount")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" "))

    val wordsPair = words.map((_,1))

    val wordsCount = wordsPair.reduceByKey(_ + _)

    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
