package actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}

import java.io.File
import fileutils.{FileAdapter, FileEvent, FileWatcher}

object Main {

	object kafkaSender {
		final case class IsolatedLogs(logs: String)

		def apply(): Behavior[IsolatedLogs] = Behaviors.receive { (context, message) =>
			context.log.info("Received message into kafkaSender from niowatcher:-")
			context.log.info(message.logs)
			Behaviors.same
		}
	}

	object niowatcher {
		final case class logfile(msg: String)

		def apply(): Behavior[logfile] =
			Behaviors.setup { context =>
				val sender = context.spawn(kafkaSender(), "test")

				Behaviors.receiveMessage { message =>

					val folder: File = new File(message.msg)
					val watcher: FileWatcher = new FileWatcher(folder)
					watcher.addListener(new FileAdapter() {
						override def onCreated(event: FileEvent): Unit = {
							context.log.info("File created " + event.getFile.getName)
						}

						override def onModified(event: FileEvent): Unit = {
							context.log.info("File modified " + event.getFile.getName)
							context.log.info("Sending message to other actor")
							sender ! kafkaSender.IsolatedLogs(message.msg)
						}
					}).watch()

					Behaviors.same
				}
			}
	}

	def main(args: Array[String]): Unit = {
		val system: ActorSystem[niowatcher.logfile] = ActorSystem(niowatcher(), "test2")

		system ! niowatcher.logfile("log")
//		system.terminate()
//		System.exit(0)
	}
}
