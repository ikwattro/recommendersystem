package com.infosupport.recommendedcontent.restservice

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, StatusCodes, StatusCode, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, ExceptionHandler}
import akka.http.scaladsl.settings.RoutingSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.infosupport.recommendedcontent.core.RecommenderSystem
import org.apache.spark.{SparkContext, SparkConf}

import akka.pattern.ask
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

import scala.concurrent.duration._

/**
  * Implementation of the rest service
  */
class RestService(interface: String, port: Int = 8080)(implicit val system: ActorSystem) extends RestServiceProtocol {
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = 30 seconds

  val config = new SparkConf()

  config.setMaster(system.settings.config.getString("spark.master"))
  config.setAppName("recommended-content-service")
  config.set("spark.cassandra.connection.host", system.settings.config.getString("cassandra.server"))

  val sparkContext = new SparkContext(config)

  val recommenderSystem = system.actorOf(RecommenderSystem.props(sparkContext))

  val errorHandler = ExceptionHandler {
    case e: Exception => complete {
      (StatusCodes.InternalServerError -> ErrorResponse("Internal server error"))
    }
  }

  val route = {
    handleExceptions(errorHandler) {
      pathPrefix("recommendations") {
        path(Segment) { id =>
          get {
            complete {
              (recommenderSystem ? RecommenderSystem.GenerateRecommendations(id.toInt))
                .mapTo[RecommenderSystem.Recommendations]
                .flatMap(result => Future {
                  (StatusCodes.OK -> result)
                })
            }
          }
        }
      } ~ path("train") {
        post {
          recommenderSystem ! RecommenderSystem.Train

          complete {
            (StatusCodes.OK -> GenericResponse("Training started"))
          }
        }
      }
    }
  }

  /**
    * Starts the HTTP server
    */
  def start(): Unit = {
    Http().bindAndHandle(route, interface, port)
  }
}
