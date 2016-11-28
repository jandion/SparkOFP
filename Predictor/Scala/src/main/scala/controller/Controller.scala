package controller

import com.typesafe.config.ConfigFactory
import core.Predictor
import iomanager.{InputManager, OutputManager}
import modelmanager.ModelFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Controller {
  def main(args: Array[String]) {
    val defaultConfig = ConfigFactory.load("application.conf")
    val config = ConfigFactory.load.withFallback(defaultConfig)
    val eventsCount = config.getInt("eventsCount")

    //Set up spark environment
    val conf = new SparkConf().setAppName("Predictor")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(config.getInt("input.streaming.batchTime")))
    Logger.getRootLogger.setLevel(Level.ERROR)
    val models = ModelFactory.prepareModels(ssc, config)
    Predictor.setUpModels(ssc, models)
    val stream = InputManager.createInputStream(ssc, config)
    OutputManager.prepareOutputStream(config)
    stream.foreachRDD((rdd,time) => {
          val arr = rdd.reduce(_++_).toArray.sorted
          val predictions = Predictor.getPredictions(
            new SparseVector(eventsCount, arr, Array.fill(arr.length)(1.0)))
          OutputManager.sendPredictions(predictions.toParArray.partition(_._2 == 1.0),time)
      })
    ssc.start()
    ssc.awaitTermination()

  }

}