package iomanager

import java.util

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.Time

import scala.collection.JavaConversions._
import scala.collection.parallel.mutable.ParArray

object OutputManager {

  var producer: KafkaProducer[String, String] = null
  var predictionWindow = 0

  def prepareOutputStream(config: Config) = {

    predictionWindow = config.getInt("output.predictionWindow")*1000

    val brokers = config.getStringList("output.kafka.brokers").reduce(_ + "," + _)

    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    producer = new KafkaProducer[String, String](props)
  }

  def sendPredictions(predictions: (
    ParArray[(String, Double, String, String)],
      ParArray[(String, Double, String, String)]), time: Time) = {
    val simplePredictions =
      "{\"predictionStart\":"+time.milliseconds+
        ",\"predictionEnd\":"+(time.milliseconds+predictionWindow)+
      ",\"positive\":["+predictions._1.map(_._3).mkString(",")+
      "],\"negative\":["+predictions._2.map(_._3).mkString(",")+"]}"
    val advancedPredictions =
      "{\"predictionStart\":"+time.milliseconds+
        ",\"predictionEnd\":"+(time.milliseconds+predictionWindow)+
      ",\"positive\":["+predictions._1.map(_._4).mkString(",")+
      "],\"negative\":["+predictions._2.map(_._4).mkString(",")+"]}"

    val simpleMess =
      new ProducerRecord[String, String]("simple-predictions",simplePredictions)
    val advancedMess =
      new ProducerRecord[String, String]("advanced-predictions",advancedPredictions)

    producer.send(simpleMess)
    producer.send(advancedMess)
  }

}

