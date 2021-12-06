package actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties
import org.apache.kafka.clients.producer._

import java.io.File
import fileutils.{FileAdapter, FileEvent, FileWatcher}

import scala.collection.mutable.Map
import scala.io.Source

object Main {

	val config: Config = ConfigFactory.load("application.conf")
	val appConfig = config.getConfig("app")

	// This actor is responsible for publishing received messages to the configured Kafka instance
	object kafkaSender {
		final case class IsolatedLogs(logs: String)

		def apply(): Behavior[IsolatedLogs] = Behaviors.receive { (context, message) =>
			context.log.info("Received message into kafkaSender from msgParser:-")

			val props = new Properties()
			props.put("bootstrap.servers", appConfig.getString("kafka.host"))
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			val producer = new KafkaProducer[String, String](props)
			val record = new ProducerRecord[String, String](appConfig.getString("kafka.topic"), appConfig.getString("kafka.key"), message.logs)

			//Push message to Kafka and close the producer
			producer.send(record)
			producer.close()
			Behaviors.same
		}
	}

	// This actor is responsible for parsing the received log messages and binning it into time windows.
	// Further, it also counts the ERROR or WARN messages and only forwards them to the KafkaSend actor if they are more than a threshold amount
	object msgParser {
		final case class logfile(msg: String)

		def apply(): Behavior[logfile] =
			Behaviors.setup { context =>
				val sender = context.spawn(kafkaSender(), "Kafka")

				Behaviors.receiveMessage { message =>

					context.log.info("Received message in msgParser")

					// Hash maps for processing
					val timeBins: Map[String, String] = Map()
					val countBins: Map[String, Int] = Map()
					val bufferedSource = Source.fromFile(message.msg)
					bufferedSource.getLines().foreach { item =>
						if(item.contains("ERROR") || item.contains("WARN")) {
							val nextMinuteBin = String.format("%02d", item.substring(3, 5).toInt + 1)
							val hourBin = item.substring(0, 2)

							// Edge case when it is 23:59 in the log message with overflow minute and hour
							if(nextMinuteBin == "60" && hourBin == "23") {
								if(timeBins.contains("23:59-00:00")) {
									timeBins.update("23:59-00:00", timeBins("23:59-00:00") + "\n" + item)
									countBins.update("23:59-00:00", countBins("23:59-00:00") + 1)
								} else {
									timeBins += ("23:59-00:00" -> item)
									countBins += ("23:59-00:00" -> 1)
								}
							}

							// Edge case for whenever the minute is :59
							else if(nextMinuteBin == "60") {
								val formulated = item.substring(0, 5) + "-" + String.format("%02d", item.substring(0, 2).toInt + 1) + ":00"
								if(timeBins.contains(formulated)) {
									timeBins.update(formulated, timeBins(formulated) + "\n" + item)
									countBins.update(formulated, countBins(formulated) + 1)
								} else {
									timeBins += (formulated -> item)
									countBins += (formulated -> 1)
								}
							}
							else {
								val formulated = item.substring(0, 5) + "-" + item.substring(0, 3) + String.format("%02d", item.substring(3, 5).toInt + 1)
								if(timeBins.contains(formulated)) {
									timeBins.update(formulated, timeBins(formulated) + "\n" + item)
									countBins.update(formulated, countBins(formulated) + 1)
								} else {
									timeBins += (formulated -> item)
									countBins += (formulated -> 1)
								}
							}
						}
					}

					// Look for higher than threshold amount of ERROR or WARN messages
					countBins.keys.foreach { i =>
						if(countBins(i) > appConfig.getInt("thresholdAmount"))
							sender ! kafkaSender.IsolatedLogs(timeBins(i))
					}
					context.log.info("Finished parsing in msgParser")
					Behaviors.same
				}
			}
	}

	// This actor is responsible for watching a directory using NIO and and alerting the other actor with the modified file details
	object niowatcher {
		final case class logDir(msg: String)

		def apply(): Behavior[logDir] =
			Behaviors.setup { context =>
				val sender = context.spawn(msgParser(), "WatchFolder")

				Behaviors.receiveMessage { message =>

					// Utility interfaces derived from package fileutils
					val folder: File = new File(message.msg)
					val watcher: FileWatcher = new FileWatcher(folder)
					watcher.addListener(new FileAdapter() {
						override def onCreated(event: FileEvent): Unit = {
							context.log.info("File created " + event.getFile.getName)
							context.log.info("Sending message to Message Parser")
							sender ! msgParser.logfile(message.msg + "/" + event.getFile.getName)
						}

						override def onModified(event: FileEvent): Unit = {
							context.log.info("File modified " + event.getFile.getName)
							context.log.info("Sending message to Message Parser")
							sender ! msgParser.logfile(message.msg + "/" + event.getFile.getName)
						}
					}).watch()

					Behaviors.same
				}
			}
	}

	def main(args: Array[String]): Unit = {
		val system: ActorSystem[niowatcher.logDir] = ActorSystem(niowatcher(), "MainWatcher")

		// Pass directory info which is to be watched for changes/creation
		system ! niowatcher.logDir(appConfig.getString("watchFolder"))

		// Graceful shutdown
		Runtime.getRuntime.addShutdownHook(new Thread() {override def run = {
			println("Terminating actor system through addShutDownHook handler")
			system.terminate()
		}})
	}
}
