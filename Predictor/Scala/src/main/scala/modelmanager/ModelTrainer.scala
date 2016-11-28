package modelmanager

import java.io.File

import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

object ModelTrainer {
  val nClasses: Int = 2
  val positiveLabel: Double = 1.0
  val negativeLabel: Double = 0.0
  val maxBins: Int = 100

  def trainModels(ssc: StreamingContext, config: Config) = {

    //Load configuration
    val depth = config.getInt("models.trainingConfiguration.depth")
    val impurity = config.getString("models.trainingConfiguration.impurity")
    val strategy = config.getString("models.trainingConfiguration.strategy")
    val seed = config.getInt("models.trainingConfiguration.seed")
    val forestSize = config.getInt("models.trainingConfiguration.forestSize")
    val dataPath = config.getString("models.trainingConfiguration.pathToTrainingData")
    val modelsPath = config.getString("models.pathToModels")
    val events = config.getStringList("models.models")
    val categoricalInfo = Range(0, config.getInt("eventsCount")).map((_, 2)).toMap

    val models = events.par.map(modelName => {
      (modelName,
        RandomForest.trainClassifier(
          MLUtils.loadLibSVMFile(ssc.sparkContext, dataPath + modelName + ".libsvm"),
          nClasses,
          categoricalInfo,
          forestSize,
          strategy,
          impurity,
          depth,
          maxBins,
          seed))
    })

    if (config.getBoolean("models.saveModels"))
      models.seq.foreach(x => {
        FileUtils.deleteQuietly(new File(modelsPath + x._1))
        x._2.save(ssc.sparkContext, modelsPath + x._1)
      })
    models
  }
}
