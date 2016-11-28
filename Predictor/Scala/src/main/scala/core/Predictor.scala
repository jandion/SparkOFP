package core

import kafka.utils.Json
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.streaming.StreamingContext

import scala.collection.parallel.mutable.ParSeq

object Predictor {


  var models: ParSeq[(String, RandomForestModel)] = null

  def setUpModels(ssc: StreamingContext, models: ParSeq[(String, RandomForestModel)]) = {
    this.models=models
  }

  def getPredictions(v: Vector) = {
    models.map(model => {
      val pred = model._2.predict(v)
      (
        model._1,
        pred,
        Json.encode(Map("modelName"->model._1,"prediction"->pred)),
        Json.encode(
        Map(
          "modelName" -> model._1,
          "numTrees" -> model._2.numTrees,
          "totalNodes" -> model._2.totalNumNodes,
          "prediction" -> pred,
          "trees" -> model._2.trees.par.map(tree =>
            Map("nodes" -> tree.numNodes, "prediction" -> tree.predict(v))).toArray
        )
        )
        )
    }
    )
  }
}