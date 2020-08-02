import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    val conf = ConfigFactory.load()
    val broker = conf.getConfig(args(0))
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker.getString("bootstrap.server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka_Producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
    val data = new ProducerRecord[String, String]("Kafka-Testing", args(1),"kdnksdnfkndfkndfkv")
    producer.send(data)
    producer.close()
  }

}
