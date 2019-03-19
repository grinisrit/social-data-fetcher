package servicetools

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import apisources.youtube.{YoutubeEngine, YoutubeStream}
import org.mongodb.scala.{MongoClient, MongoDatabase}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


object ServiceManager {

  case object StopService

  def props(mongoDBHost: String,
            workSeed: Option[String],
            sourceConfigurations: Vector[SourceConfiguration]) =
    Props(classOf[ServiceManager], mongoDBHost, workSeed, sourceConfigurations)

  def stringMessageDecoder(message: String): Vector[DataSource] = {
    message.split(",").toVector.map{
      case src if src.contains("youtube:") => YoutubeSource(src.substring(8))
      case str: String                     => UnknownDataSource(str)
    }
  }

}

class ServiceManager(mongoDBHost: String,
                     workSeed: Option[String],
                     sourceConfigurations: Vector[SourceConfiguration]) extends Actor with ActorLogging {

  import ServiceManager._
  import MongoDBConnector._
  import context.dispatcher

  log.info(s"Starting service with tasks: $workSeed ...")

  val mongoClient: MongoClient = MongoClient(s"mongodb://$mongoDBHost")
  val dataBase: MongoDatabase = mongoClient.getDatabase("socialdata")
  val mongoDBConnector: ActorRef = context.actorOf(MongoDBConnector.props(mongoDBHost, self), "mongodb-connector")
  val dbConnectionChecker: Cancellable = context.system.scheduler.schedule(
    0.seconds, 5.minutes, mongoDBConnector, CheckConnectionForClient(mongoClient))

  val sourceEngines: Map[DataSource, ActorRef] = buildSourceEngines(sourceConfigurations)
  if(sourceEngines.isEmpty) {
    log.error("No source engines available")
    self ! StopService
  }

  val pullSystem: ActorRef = context.actorOf(PullSystem.props(sourceEngines,4), "pull-system")
  workSeed.foreach(self ! _)

  def receive: Receive = {
    case message: String =>
      stringMessageDecoder(message).foreach(pullSystem ! _)
    case StopService =>
      log.info("Stopping service ...")
      mongoClient.close()
      Await.ready(context.system.terminate(), 2.seconds)
  }

  def buildSourceEngines(sourceConfigurations: Vector[SourceConfiguration]): Map[DataSource, ActorRef] = {

    var srcEng = mutable.Map.empty[DataSource, ActorRef]

    sourceConfigurations.foreach{
      case conf: YoutubeConfiguration =>
        Try { new YoutubeEngine(conf) } match {
          case Success(engine) =>
            srcEng += YoutubeSource ->
              context.actorOf(YoutubeStream.props(dataBase.getCollection("youtubecomments"), engine), engine.getApplicationName)
          case Failure(e) =>
            log.error(s"Youtube source unavailable: $e")
        }
    }

    srcEng.toMap
  }

}


