import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.reflect.io.Path

object Main {

  val nClasses: Int = 2
  val positiveLabel: Double = 1.0
  val negativeLabel: Double = 0.0
  val categoricalInfo = Range(0, 575).map(_ -> 2).toMap
  val maxBins: Int = 100

  def main(args: Array[String]) {
    val defaultConfig =ConfigFactory.load("applicationDefault.conf")
    val config = ConfigFactory.load.withFallback(defaultConfig)

    //Load configuration
    val depth = Range(config.getInt("trainingConfigurations.minDepth"),
      config.getInt("trainingConfigurations.maxDepth")+1)
    val impurity = config.getStringList("trainingConfigurations.impurity")
    val strategy = config.getStringList("trainingConfigurations.strategy")
    val seed: Int = config.getInt("trainingConfigurations.seed")
    val forestSize = config.getIntList("trainingConfigurations.forestSize")
    val partitions: Int = config.getInt("trainingConfigurations.partitions")
    val partitionWeights: Array[Double] = Array.fill(partitions) {
      1.0 / partitions
    }
    val partitionsRange = Range(0, partitions)

    //Prepare spark environment
    val conf = new SparkConf().setAppName("ModelSelector")
    val sc = new SparkContext(conf)
    val log = LoggerFactory.getLogger("ModelSelector")

    //Calculate all possible training configurations
    val dataPath = config.getString("input.pathToData")
    val criticalEvents = config.getStringList("input.models").map(x => dataPath + x + ".libsvm")
    val trainConfigurations = for (_1 <- depth; _2 <- impurity; _3 <- strategy; _4 <- forestSize)
      yield (_1, _2, _3, _4)
    log.info(s"Configurations loaded: ${trainConfigurations.length}")
    log.info(s"Events to train: ${criticalEvents.length}")
    log.info(s"Partitions in cross-validation: $partitions")
    log.info(s"Total trainings: ${partitions*criticalEvents.length*trainConfigurations.length}")


    val globalResults = criticalEvents.par
      .map(x => (x, MLUtils.loadLibSVMFile(sc, x)))
      .flatMap{
        case (modelName, modelData) => {
          val dataPartitioned = modelData
            .filter(_.label == positiveLabel).randomSplit(partitionWeights, seed)
            .zip(modelData.filter(_.label == negativeLabel).randomSplit(partitionWeights, seed))
            .map(x => x._1 ++ x._2)
            .zipWithIndex

          partitionsRange.flatMap(f = partition => {
            val (trainingSet, testingSet) = (
              dataPartitioned.filterNot(_._2 == partition).map(_._1).reduce(_ ++ _),
              dataPartitioned.filter(_._2 == partition).map(_._1).reduce(_ ++ _))
            trainConfigurations.par.map {
              case (_depth, _impurity, _strategy, _numTrees) => {

                val t0 = System.currentTimeMillis()
                val model = RandomForest.trainClassifier(
                  trainingSet,
                  nClasses,
                  categoricalInfo,
                  _numTrees,
                  _strategy,
                  _impurity,
                  _depth,
                  maxBins,
                  seed)

                val t1 = System.currentTimeMillis()
                val predictionAndLabels = testingSet.map { y =>
                  val prediction = model.predict(y.features)
                  (prediction, y.label)
                }
                val metrics = new MulticlassMetrics(predictionAndLabels)
                var fScore: Double = Double.NegativeInfinity
                try {
                  fScore = metrics.fMeasure(positiveLabel)
                } catch {
                  case e: Exception => {}
                }

                val t2 = System.currentTimeMillis()
                log.info(Console.BOLD + Console.GREEN + Console.BLACK_B
                  +(fScore, _numTrees, _strategy, _impurity, _depth,
                  modelName, partition, t1 - t0, t2 - t0, t2 - t1)
                  + Console.RESET)
                (fScore, _numTrees, _strategy, _impurity, _depth,
                  modelName, partition, t1 - t0, t2 - t0, t2 - t1)
              }
            }
          })

        }
      }
    Path("GResults.txt").createFile().writeAll(globalResults.mkString("\n"))

  }
}

