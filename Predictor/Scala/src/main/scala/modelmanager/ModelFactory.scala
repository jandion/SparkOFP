package modelmanager

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext


object ModelFactory {

  /**
   * Prepares the list of RandomForestModels which will be given to Predictor
   *
   * @param ssc Spark Context
   * @param config Application config
   * @return random forest models list
   */
  def prepareModels(ssc: StreamingContext, config: Config) = {
    if (config getBoolean "models.trainNewModels")
      ModelTrainer.trainModels(ssc, config)
    else
      ModelLoader.readModels(ssc, config)
  }
}
