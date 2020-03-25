import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

object Program extends App {
  val producer = createProducer[Long, String]

  5000 times { i =>
    val record = new ProducerRecord[Long, String]("demo", s"This is record ${i}")
    try {
      val metadata = producer.send(record).get
      println(s"Record sent with key $i to partition ${metadata.partition} with offset ${metadata.offset}")
    } catch {
      case e: ExecutionException =>
        println(s"Execution exception: ${e.getMessage}")
      case e: InterruptedException =>
        println(s"Interrupted: ${e.getMessage}")
      case e =>
        println(s"Other exception: ${e.getMessage}")
    }
  }

  private def createProducer[A, B]: KafkaProducer[A, B] = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client1")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, reflect.classTag[LongSerializer].runtimeClass.getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, reflect.classTag[StringSerializer].runtimeClass.getName)
    new KafkaProducer[A, B](properties)
  }

  implicit class IntExtensions(x: Int) {
    def times[A](f: Int => A): Unit = (0 to x).foreach(f)
  }
}
