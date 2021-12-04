
import org.apache.spark._
import org.apache.log4j._

import java.util.Properties
import scala.collection.Map
import Helper._
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.{File, PrintWriter}
import java.time.Duration
import java.util
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter


object SparkLogs {

  //Generates String report based on spark processing
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

    val logger = Logger.getLogger("org")

    //Set up kafka and subscribe to logfilescraper topic
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("logfilescraper"))

    logger.info("Subscribed to kafka topic")

    //get spark context
    val sc = new SparkContext("local[*]", "SparkLogs")

    logger.info("retrieved spark context")

    //Run kafka listener
    while (true) {

      //check every 2 seconds for new messages from kafka
      val record = consumer.poll(Duration.ofMillis(2000)).asScala

      logger.info("polling for kafka messages")

      //when messages received begin processing
      if(!record.isEmpty) {

        logger.info("kafka message received")

        //write kafka message to file
        val pw = new PrintWriter(new File("kafkatest.txt" ))
        for (data <- record.iterator) {
          pw.write(data.value())
        }
        pw.close

        logger.info("kafka message written to file")

        logger.info("beginning spark processing")

        //create rdd from file
        val lines = sc.textFile("kafkatest.txt")

        //get number of error and warn messages
        val rddType = lines.map(parseType)
        val countType = rddType.countByValue()

        //get start and end time of logs
        val rddTime = lines.map(parseTime)
        val start = rddTime.min()
        val end = rddTime.max()

        //take first 5 logs
        val first5sample = lines.take(5)

        //get last 5 logs
        val reversedLastSamples = lines
            .zipWithIndex()
            .map({ case (x, y) => (y, x) })
            .sortByKey(ascending = false)
            .map({ case (x, y) => y })
            .take(5)
        val last5Sample = reversedLastSamples.reverse

        logger.info("spark processing complete")

        //generate report and send email to stakeholder
        sendEmail(generateReport(countType, start, end, first5sample, last5Sample))
        logger.info("email sent successfully")
      }
    }

  }


}
