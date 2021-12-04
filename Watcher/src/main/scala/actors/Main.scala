package actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import java.util.Properties
import org.apache.kafka.clients.producer._
import java.io.File
import fileutils.{FileAdapter, FileEvent, FileWatcher}
import scala.collection.mutable.Map
import scala.io.Source

object Main {

	object kafkaSender {
		final case class IsolatedLogs(logs: String)

		def apply(): Behavior[IsolatedLogs] = Behaviors.receive { (context, message) =>
			context.log.info("Received message into kafkaSender from msgParser:-")
			context.log.info(message.logs)

			val props = new Properties()
			props.put("bootstrap.servers", "localhost:9092")
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			val producer = new KafkaProducer[String, String](props)
			val record = new ProducerRecord[String, String]("logfilescraper", "RetrieveLogs", message.logs)
			producer.send(record)
			producer.close()
			Behaviors.same
		}
	}

	object msgParser {
		final case class logfile(msg: String)

		def apply(): Behavior[logfile] =
			Behaviors.setup { context =>
				val sender = context.spawn(kafkaSender(), "Kafka")

				Behaviors.receiveMessage { message =>

					context.log.info("Received message in msgParser")
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

						//						println(item)
					}
//					System.out.println(timeBins)
//					System.out.println(countBins)
					countBins.keys.foreach { i =>
						if(countBins(i) > 4)
							sender ! kafkaSender.IsolatedLogs(timeBins(i))
					}
					context.log.info("Finished parsing in msgParser")
					Behaviors.same
				}
			}
	}

	object niowatcher {
		final case class logDir(msg: String)

		def apply(): Behavior[logDir] =
			Behaviors.setup { context =>
				val sender = context.spawn(msgParser(), "test")

				Behaviors.receiveMessage { message =>

					val folder: File = new File(message.msg)
					val watcher: FileWatcher = new FileWatcher(folder)
					watcher.addListener(new FileAdapter() {
						override def onCreated(event: FileEvent): Unit = {
							context.log.info("File created " + event.getFile.getName)
							sender ! msgParser.logfile(message.msg + "/" + event.getFile.getName)
						}

						override def onModified(event: FileEvent): Unit = {
							context.log.info("File modified " + event.getFile.getName)
							context.log.info("Sending message to other actor")
							sender ! msgParser.logfile(message.msg + "/" + event.getFile.getName)
						}
					}).watch()

					Behaviors.same
				}
			}
	}

	def main(args: Array[String]): Unit = {
		val system: ActorSystem[niowatcher.logDir] = ActorSystem(niowatcher(), "test2")

		system ! niowatcher.logDir("log")
//		system.terminate()
//		System.exit(0)
	}
}
