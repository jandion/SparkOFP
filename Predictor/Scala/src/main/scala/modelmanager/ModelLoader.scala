package modelmanager

import com.typesafe.config.Config
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

object ModelLoader {
  def readModels(ssc: StreamingContext, config: Config) = {

    val modelsPath = config.getString("models.pathToModels")
    val events = config.getStringList("models.models")

    events.par.map(eventName => {
      (eventName, RandomForestModel.load(ssc.sparkContext, modelsPath + eventName))
    })
  }
}