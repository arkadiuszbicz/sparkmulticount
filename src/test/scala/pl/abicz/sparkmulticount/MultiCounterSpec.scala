package pl.abicz.sparkmulticount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest._

class MultiCounterSpec extends WordSpecLike with BeforeAndAfterAll with Matchers {

  import MultiCounter._

  val conf = new SparkConf().setAppName("Spark Multi Count")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val geoRes = this.getClass().getResource("/GeoLiteCity.dat").getFile

  override def afterAll(): Unit = {
    sc.stop()
  }

  "A MultiCounter " must {

    "parse propely Cvs " in {

      val testData = List("wrongline",
        "1,216.58.209.78,view",
        "1,23232.232.32",
        "2,213.180.141.140,impression", "", " ").iterator
      val out = toInputWithCountry(getIpLookups(geoRes), testData)
      out.toList should be(List((VideoCountryKey("1", "US"), "view"),
        (VideoCountryKey("2", "PL"), "impression")))
    }

    "combine properly" in {

      val input = sc.parallelize(List(
        "1,216.58.209.78,view",
        "2,213.180.141.140,impression",
        "1,216.58.209.78,view",
        "2,213.180.141.140,impression",
        "1,216.58.209.78,view",
        "1,216.58.209.78,click",
        "1,216.58.209.78,view",
        "2,213.180.141.140,impression",
        "2,213.180.141.140,click"))
      val multiCount = new MultiCounter(geoRes)
      val result = multiCount.multiCount(input)

      result.toLocalIterator.toList should be (List((VideoCountryKey("1","US"),Metrics(4,1,0)), (VideoCountryKey("2","PL"),Metrics(0,1,3))))
    }
  }
}
