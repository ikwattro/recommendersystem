package com.infosupport.recommendedcontent.core

import java.io.Serializable

import akka.actor.{Props, Actor, ActorLogging}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
  * Companion object for the RecommenderSystem
  */
object RecommenderSystem {
  case object Train {}
  case class GenerateRecommendations(userId: Int)
  case class Recommendation(contentItemId: Int, rating: Double)
  case class Recommendations(items: Seq[Recommendation])

  /**
    * Defines the properties for the recommender system actor
    * @param sc Spark context to use
    * @return   Returns the properties for the actor
    */
  def props(sc: SparkContext) = Props(new RecommenderSystem(sc))
}

/**
  * Implements a trainable recommender system
  * @param sc Spark context to use
  */
class RecommenderSystem(sc: SparkContext) extends Actor with ActorLogging {

  import RecommenderSystem._

  var model: Option[MatrixFactorizationModel] = None

  // Try to load the existing model data.
  // This makes the service usable right away after it was down before, but was trained.
  loadExistingModel()

  def receive = {
    case Train => trainModel()
    case GenerateRecommendations(userId) => generateRecommendations(userId, 10)
    case ModelTrainer.TrainingResult(model) => storeModel(model)
  }

  /**
    * Trains a new version of the recommender system model
    * @return Returns the trained model
    */
  private def trainModel() = {
    // Start a separate actor to train the recommendation system.
    // This enables the service to continue service requests while it learns new recommendations.
    val trainer = context.actorOf(ModelTrainer.props(sc), "model-trainer")
    trainer ! ModelTrainer.Train
  }

  private def storeModel(model: MatrixFactorizationModel) = {
    this.model = Some(model)

    // Store the model in HDFS in case the service is stopped and started
    // after the model was trained.
    val fileHost = context.system.settings.config.getString("hdfs.server")

    model.save(sc, s"hdfs://${fileHost}/recommendations/")
  }

  /**
    * Preloads an existing model for the service.
    */
  private def loadExistingModel() = {
    val fileHost = context.system.settings.config.getString("hdfs.server")

    val fileConfig = new Configuration()
    fileConfig.set("fs.default.name",s"hdfs://${fileHost}/")

    val fs = org.apache.hadoop.fs.FileSystem.get(fileConfig)

    // Make sure that the model exists.
    // And even if it exists, load with caution, it could break.
    if(fs.exists(new Path("/recommendations/data/"))) {
      try {
        val loadedModel = MatrixFactorizationModel.load(sc,s"hdfs://${fileHost}/recommendations/")

        log.info("Reloaded the model from HDFS, repartitioning and caching the data")

        // Cache the product features when the model is loaded.
        // This improves the performance of the recommender system quite a bit.
        loadedModel.productFeatures.cache().repartition(sc.defaultMinPartitions)
        loadedModel.userFeatures.repartition(sc.defaultMinPartitions)

        this.model = Some(loadedModel)
      }
      catch {
        case e: Exception => log.warning("There is a model available, but it is unloadable")
      }
    }
  }

  /**
    * Generates recommendations
    * @param userId User ID for which to generate recommendations
    */
  private def generateRecommendations(userId: Int, count: Int) = {
    log.info(s"Generating ${count} recommendations for user with ID ${userId}")

    // Generate recommendations based on the machine learning model.
    // When there's no trained model return an empty list instead.
    val results = model
      .flatMap(m => Some(m.recommendProducts(userId,count).toList))
      .flatMap(recommendations => Some(recommendations.map(rating => Recommendation(rating.product,rating.rating))))
      .getOrElse(Nil)

    sender ! Recommendations(results)
  }
}
