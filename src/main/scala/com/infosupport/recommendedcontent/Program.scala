package com.infosupport.recommendedcontent

import akka.actor.ActorSystem
import akka.util.Timeout
import com.infosupport.recommendedcontent.restservice.RestService
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._

/**
  * Entrypoint for the application
  */
object Program extends App {
  implicit val system = ActorSystem("recommended-content-service")
  implicit val timeout: Timeout = 30 seconds

  val service = new RestService("localhost")

  service.start()
}


object MyApp extends App {
  val config = new SparkConf()

  config.setMaster("local[*]")
  config.setAppName("MySparkApp")

  val sc = new SparkContext(config)

  val records = sc.textFile("Somefile.csv")
    .map(line => line.split(","))
    .map(item => SomeData(item(0),item(1)))

  records.count()
}
