
import org.apache.spark._
import org.apache.log4j._

import java.util.Properties
import scala.collection.Map
import javax.mail._
import javax.mail.internet._
import com.typesafe.config.ConfigFactory
import org.apache.spark
import Helper._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SQLContext

import java.io.{File, PrintWriter}
import java.time.Duration
import java.util
import scala.collection.immutable.HashMap
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter


object SparkLogs {
  def generateReport(count:Map[String, Long], start:Long, end:Long,
                     first5sample: Array[String], last5sample: Array[String]): String =
  {
    val bodyLine1 = "This report is generated for logs between the time frame: "+
      convertMilliToTime(start) + " --- " + convertMilliToTime(end) + "\n"
    val bodyLine2 = "There was: "  + count.get("ERROR").get + " ERROR messages\n"
    val bodyLine3 = "There was: "  + count.get("WARN").get + " WARN messages\n\n\n"
    val bodyLine4 = "First 5 logs in this time period:\n"
    val bodyLine5 =  "\n\n\n Last 5 Logs in this time period\n"
    val first5sampleOfLogs = first5sample.mkString("\n")
    val last5sampleOfLogs = last5sample.mkString("\n")
    val body = bodyLine1 + bodyLine2 + bodyLine3 + bodyLine4 + first5sampleOfLogs + bodyLine5 + last5sampleOfLogs
    body
  }
  def main(args: Array[String]): Unit =
  {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("logfilescraper"))
    while (true) {
      val record = consumer.poll(Duration.ofMillis(2000)).asScala
//      System.out.println(record.isEmpty)
      if(!record.isEmpty) {

        val pw = new PrintWriter(new File("kafkatest.txt" ))
        for (data <- record.iterator) {
//          println(data.value())
          pw.write(data.value())
        }
//        pw.write(record.)
        pw.close
        val logger = Logger.getLogger("org")

        val sc = new SparkContext("local[*]", "SparkLogs")

        val lines = sc.textFile("kafkatest.txt")

        val rddType = lines.map(parseType)
        val countType = rddType.countByValue()

        val rddTime = lines.map(parseTime)
        val start = rddTime.min()
        val end = rddTime.max()

        val first5sample = lines.take(5)
        val reversedLastSamples = lines
            .zipWithIndex()
            .map({ case (x, y) => (y, x) })
            .sortByKey(ascending = false)
            .map({ case (x, y) => y })
            .take(5)

        val last5Sample = reversedLastSamples.reverse


        System.out.println(lines.take(1)(0))
        sendEmail(lines.take(1)(0))
//        sendEmail(generateReport(countType, start, end, first5sample, last5Sample))
      }
//      for (data <- record.iterator)
//        println(data.value())
    }
//    val logger = Logger.getLogger("org")
//
//    val sc = new SparkContext("local[*]", "SparkLogs")
//
//    val lines = sc.textFile("./src/main/resources/test.txt")
//
//    val rddType = lines.map(parseType)
//    val countType = rddType.countByValue()
//
//    val rddTime = lines.map(parseTime)
//    val start = rddTime.min()
//    val end = rddTime.max()
//
//    val first5sample = lines.take(5)
//    val reversedLastSamples = lines
//      .zipWithIndex()
//      .map({ case (x, y) => (y, x) })
//      .sortByKey(ascending = false)
//      .map({ case (x, y) => y })
//      .take(5)
//
//    val last5Sample = reversedLastSamples.reverse
//
//
//
//
//    sendEmail(generateReport(countType, start, end, first5sample, last5Sample))
  }


}
