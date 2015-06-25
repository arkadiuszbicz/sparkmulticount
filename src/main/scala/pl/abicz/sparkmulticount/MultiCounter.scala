package pl.abicz.sparkmulticount

import com.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import com.snowplowanalytics.maxmind.iplookups.IpLookups

object MultiCounter {

  case class InputData(video_id: String, ip: String, eventType: String)

  case class VideoCountryKey(video_id: String, countryCode: String)

  case class Metrics(view: Long, click: Long, impression: Long) extends Serializable {
    def getMetrics(metric: String): Metrics = {
      if (metric == "view")
        Metrics(view + 1, click, impression)
      else if (metric == "click")
        Metrics(view, click + 1, impression)
      else if (metric == "impression")
        Metrics(view, click, impression + 1)
      else this
    }

    def merge(metrics: Metrics): Metrics = {
      Metrics(metrics.view + view, metrics.click + click, metrics.impression + impression)
    }
  }

  def toInputWithCountry(ipLookups: IpLookups, lines: Iterator[String]): Iterator[(VideoCountryKey, String)] = {
    val separator: String = ","
    val parser = new CSVParser(',')
    val localIpLookups = ipLookups;
    lines.flatMap { line => {
      val columns = parser.parseLine(line)
      if (columns.length == 3) {
        val input = InputData(columns(0), columns(1), columns(2))
        val countryCode = localIpLookups.performLookups(input.ip)._1.
          map(_.countryCode).getOrElse("Unknown")
        Some((VideoCountryKey(input.video_id, countryCode), input.eventType))
      }
      else None
    }
    }
  }

  def getIpLookups(geoFile : String) = {
    IpLookups(geoFile = Some(geoFile), ispFile = None,
      orgFile = None, domainFile = None, memCache = true, lruCache = 20000)
  }
}

class MultiCounter(geoFile : String) extends Serializable{

  import MultiCounter._

  def multiCount(txtRdd: RDD[String]): RDD[(VideoCountryKey, Metrics)] = {

    def initial(v: String) = Metrics(0, 0, 0).getMetrics(v)
    def partMerging(acc: Metrics, v: String) = acc.getMetrics(v)
    def multiPartMerging(acc1: Metrics, acc2: Metrics) = acc1.merge(acc2)

    txtRdd.mapPartitions(lines => {
      val ipLookups = getIpLookups(geoFile)
      toInputWithCountry(ipLookups, lines)
    }).combineByKey(
        initial,
        partMerging,
        multiPartMerging)
  }

}
