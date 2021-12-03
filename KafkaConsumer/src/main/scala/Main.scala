import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.language.postfixOps

object Main {

	def main(args: Array[String]): Unit = {
		consumeFromKafka("logfilescraper")
	}

	def consumeFromKafka(topic: String) = {
		val props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("auto.offset.reset", "latest")
		props.put("group.id", "consumer-group")
		val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
		consumer.subscribe(util.Arrays.asList(topic))
		while (true) {
			val record = consumer.poll(Duration.ofMillis(2000)).asScala
			System.out.println(record.isEmpty)
//			record.isEmpty
			for (data <- record.iterator)
				println(data.value())
		}

	}
}