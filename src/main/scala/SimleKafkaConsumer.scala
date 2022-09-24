import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.Deserializer

import java.time.Duration



object SimleKafkaConsumer extends App {

  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  //props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("amazon_books")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      val topicPartitions = consumer.assignment()
      consumer.seekToBeginning(topicPartitions)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
  println("end")
}
