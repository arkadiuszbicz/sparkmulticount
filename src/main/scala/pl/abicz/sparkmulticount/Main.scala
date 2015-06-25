package pl.abicz.sparkmulticount
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]) {
    println(args.mkString)
    val filesDirIn = args(0)
    val fileOut = args(1)
    val geoFile = args(2)
    val conf = new SparkConf().setAppName("Spark Multi Count")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(filesDirIn, 2)
    val multiCounter = new MultiCounter(geoFile)
    val result = multiCounter.multiCount(logData)
    val csvResult = result.repartition(1).map{case (videoContr, metrics) =>
    Array(videoContr.video_id,videoContr.countryCode,
      metrics.view, metrics.click, metrics.impression).mkString(",")}
    csvResult.saveAsTextFile(fileOut)
  }
}
