import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.Deserializer

import java.time.Duration
import java.util



object SimleKafkaConsumer extends App {

  val props:Properties = new Properties()
  props.put("group.id", "test")

  props.put("max.poll.records", 5)
  props.put("bootstrap.servers","localhost:9092")

  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "latest")
  //props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  //val topics = List("amazon_books")
  val topic = "amazon_books"
  val topicPartition0 = new TopicPartition(topic, 0)
  val topicPartition1 = new TopicPartition(topic, 1)
  val topicPartition2 = new TopicPartition(topic, 2)
  try {
    //consumer.subscribe(topics.asJava)
    consumer.assign(util.Arrays.asList(topicPartition0, topicPartition1, topicPartition2))

    val topicPartitions = consumer.assignment()

    consumer.seekToEnd(topicPartitions)
    val offset = consumer.position(topicPartition0) - 5

    consumer.seek(topicPartition0, offset)
    //val records = consumer.poll(Duration.ofSeconds(10))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(100))
      println(s"size = ${records.asScala.size}")

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
