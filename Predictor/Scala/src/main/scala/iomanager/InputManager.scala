package iomanager

import com.typesafe.config.Config
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

object InputManager {

  /**
   * Returns a stream with the vectors of each window prepared for the predictor
   *
   * @param ssc Spark Streaming Context
   * @param config Application config
   * @return
   */
  def createInputStream(ssc: StreamingContext, config: Config): DStream[Set[Int]] = {

    val windowDuration = Seconds(config.getInt("input.observationWindow"))
    val topics = config.getStringList("input.kafka.topics")
    val brokers = config.getStringList("input.kafka.brokers").reduce(_ + "," + _)
    val group = config.getString("input.kafka.group")

    val eventIndex = ssc.sparkContext.broadcast(config.getStringList("eventIndex")
      .map(_.split(",")).map(x => x.last -> x.head.toInt).toMap)
    val topicMap = topics.map((_, 2)).toMap
    val eventsStream =
      KafkaUtils.createStream(ssc, brokers, group, topicMap, StorageLevel.MEMORY_ONLY_2)
        .transform((data)=>{
          data.map(_._2).filter(eventIndex.value.contains)
            .map(eventIndex.value.get(_).get).distinct.map(x=>Set(x))
        })
      .reduce(_++_)
      .persist(StorageLevel.MEMORY_ONLY_2)

    eventsStream
      .window(windowDuration)
      .persist(StorageLevel.MEMORY_ONLY_2)
  }
}
