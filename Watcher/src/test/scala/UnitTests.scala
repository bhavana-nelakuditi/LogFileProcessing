import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}

import scala.language.postfixOps


class UnitTests extends AnyFunSuite {

	test("Unit test to check successful loading of configuration") {
		val config: Config = ConfigFactory.load("application.conf")
		val kafkahost = config.getString("app.kafka.host")
		kafkahost shouldBe a [String]
	}

	test("Unit test to check bin generation") {
		val testingString = "20:54:56.986 [scala-execution-context-global-181] ERROR HelperUtils.Parameters$ - NT1#6aT6sY6uB9wG7vbe0F8wL9m\"Irsj,"
		val nextMinuteBin = String.format("%02d", testingString.substring(3, 5).toInt + 1)
		val hourBin = testingString.substring(0, 3)
		val word = {
			if(nextMinuteBin == "60" && hourBin == "23") {
				"23:59-00:00"
			}
			else if(nextMinuteBin == "60") {
				testingString.substring(0, 5) + "-" + String.format("%02d", testingString.substring(0, 2).toInt + 1) + ":00"
			}
			else
				testingString.substring(0, 5) + "-" + testingString.substring(0, 3) + String.format("%02d", testingString.substring(3, 5).toInt + 1)
		}
		assert(word == "20:54-20:55")
	}

	test("Unit test to check bin generation edge case for minutes only") {
		val testingString = "20:59:56.986 [scala-execution-context-global-181] ERROR HelperUtils.Parameters$ - NT1#6aT6sY6uB9wG7vbe0F8wL9m\"Irsj,"
		val nextMinuteBin = String.format("%02d", testingString.substring(3, 5).toInt + 1)
		val hourBin = testingString.substring(0, 3)
		val word = {
			if(nextMinuteBin == "60" && hourBin == "23") {
				"23:59-00:00"
			}
			else if(nextMinuteBin == "60") {
				testingString.substring(0, 5) + "-" + String.format("%02d", testingString.substring(0, 2).toInt + 1) + ":00"
			}
			else
				testingString.substring(0, 5) + "-" + testingString.substring(0, 3) + String.format("%02d", testingString.substring(3, 5).toInt + 1)
		}
		assert(word == "20:59-21:00")
	}

	test("Unit test to check bin generation edge case for minutes and hours") {
		val testingString = "23:59:56.986 [scala-execution-context-global-181] ERROR HelperUtils.Parameters$ - NT1#6aT6sY6uB9wG7vbe0F8wL9m\"Irsj,"
		val nextMinuteBin = String.format("%02d", testingString.substring(3, 5).toInt + 1)
		val hourBin = testingString.substring(0, 2)
		val word = {
			if(nextMinuteBin == "60" && hourBin == "23") {
				"23:59-00:00"
			}
			else if(nextMinuteBin == "60") {
				testingString.substring(0, 5) + "-" + String.format("%02d", testingString.substring(0, 2).toInt + 1) + ":00"
			}
			else
				testingString.substring(0, 5) + "-" + testingString.substring(0, 3) + String.format("%02d", testingString.substring(3, 5).toInt + 1)
		}
		assert(word == "23:59-00:00")
	}

	test("Unit test to check amount of digits in time interval to be maintained at 2") {
		val testingString = "01:04:56.986 [scala-execution-context-global-181] ERROR HelperUtils.Parameters$ - NT1#6aT6sY6uB9wG7vbe0F8wL9m\"Irsj,"
		val nextMinuteBin = String.format("%02d", testingString.substring(3, 5).toInt + 1)
		val hourBin = testingString.substring(0, 2)
		val word = {
			if(nextMinuteBin == "60" && hourBin == "23") {
				testingString.substring(0, 5) + "-" + "00:00"
			}
			else if(nextMinuteBin == "60") {
				testingString.substring(0, 5) + "-" + String.format("%02d", testingString.substring(0, 2).toInt + 1) + ":00"
			}
			else
				testingString.substring(0, 5) + "-" + testingString.substring(0, 3) + String.format("%02d", testingString.substring(3, 5).toInt + 1)
		}
		assert(word == "01:04-01:05")
	}

	test("Unit test to check message sending to actor") {

		object niowatcher {
			final case class logDir(msg: String)

			def apply(): Behavior[logDir] =
				Behaviors.setup { context =>
					Behaviors.receiveMessage { message =>
						assert(message.msg == "Test message for unit tests")
						Behaviors.same
					}
				}
		}

		val system: ActorSystem[niowatcher.logDir] = ActorSystem(niowatcher(), "MainWatcher")

		system ! niowatcher.logDir("Test message for unit tests")
	}

	test("Unit test to forward message from actor to actor") {

		object actor1 {
			final case class tester(msg: String)

			def apply(): Behavior[tester] =
				Behaviors.setup { context =>
					Behaviors.receiveMessage { message =>
						assert(message.msg == "Test message forwarding for unit tests")
						Behaviors.same
					}
				}
		}

		object actor2 {
			final case class logDir(msg: String)

			def apply(): Behavior[logDir] =
				Behaviors.setup { context =>

					val sender = context.spawn(actor1(), "OtherActor")

					Behaviors.receiveMessage { message =>
						sender ! actor1.tester(message.msg)
						Behaviors.same
					}
				}
		}

		val system: ActorSystem[actor2.logDir] = ActorSystem(actor2(), "MainWatcher")

		system ! actor2.logDir("Test message forwarding for unit tests")
	}
}