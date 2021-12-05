
import org.apache.spark._
import org.apache.log4j._

import java.util.Properties
import scala.collection.Map
import Helper._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.{File, PrintWriter}
import java.time.Duration
import java.util
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter


object SparkLogs {

  val config: Config = ConfigFactory.load("application.conf")
  val appConfig = config.getConfig("app")

  //Generates String report based on spark processing
  def generateReport(count:Map[String, Long], start:Long, end:Long,
                     firstnsample: Array[String], lastnsample: Array[String]): String =
  {
    val bodyLine1 = "This report is generated for logs between the time frame: "+
      convertMilliToTime(start) + " --- " + convertMilliToTime(end) + "\n"
    val bodyLine2 = "There were: "  + count.get("ERROR").get + " ERROR messages\n"
    val bodyLine3 = "There were: "  + count.get("WARN").get + " WARN messages\n\n\n"
    val bodyLine4 = s"First ${appConfig.getInt("nSamples")} logs in this time period:\n"
    val bodyLine5 =  s"\n\n\nLast ${appConfig.getInt("nSamples")} Logs in this time period\n"
    val firstnsampleOfLogs = firstnsample.mkString("\n")
    val lastnsampleOfLogs = lastnsample.mkString("\n")
    val body = bodyLine1 + bodyLine2 + bodyLine3 + bodyLine4 + firstnsampleOfLogs + bodyLine5 + lastnsampleOfLogs
    body
  }

  def main(args: Array[String]): Unit =
  {


    val logger = Logger.getLogger("org")

    //Set up kafka and subscribe to the configured topic
    val props = new Properties()
    props.put("bootstrap.servers", appConfig.getString("kafka.host"))
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(appConfig.getString("kafka.topic")))

    logger.info(s"Subscribed to kafka topic- ${appConfig.getString("kafka.topic")}")

    //get spark context
    val sc = new SparkContext("local[*]", "SparkLogs")

    logger.info("Retrieved spark context")

    //Run kafka listener
    while (true) {

      //check every pollingPeriod seconds for new messages from kafka
      val record = consumer.poll(Duration.ofMillis(appConfig.getLong("kafka.pollingPeriod"))).asScala

      logger.info("Polling kafka for messages")

      //Upon receiving messages, begin processing
      if(!record.isEmpty) {

        logger.info("A message has been retrieved from kafka")

        //write kafka message to file
        val pw = new PrintWriter(new File(appConfig.getString("kafka.outputfile")))
        for (data <- record.iterator) {
          pw.write(data.value())
        }
        pw.close

        logger.info("kafka message written to file")

        logger.info("Beginning spark processing")

        //create rdd from file
        val lines = sc.textFile(appConfig.getString("kafka.outputfile"))

        //get number of error and warn messages
        val rddType = lines.map(parseType)
        val countType = rddType.countByValue()

        //get start and end time of logs
        val rddTime = lines.map(parseTime)
        val start = rddTime.min()
        val end = rddTime.max()

        //Take first n logs (n configured in config file)
        val firstnsample = lines.take(appConfig.getInt("nSamples"))

        //Get last n logs
        val reversedLastSamples = lines
            .zipWithIndex()
            .map({ case (x, y) => (y, x) })
            .sortByKey(ascending = false)
            .map({ case (x, y) => y })
            .take(appConfig.getInt("nSamples"))
        val lastnSample = reversedLastSamples.reverse

        logger.info("Spark processing complete")

        //generate report and send email to stakeholder
        sendEmail(generateReport(countType, start, end, firstnsample, lastnSample))
        logger.info("Email sent successfully")
        pw.write("")
        pw.close
      }
    }

  }


}
