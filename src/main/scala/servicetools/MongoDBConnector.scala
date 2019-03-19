package servicetools

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{MongoClient, MongoCollection}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object MongoDBConnector {
  case class CheckConnectionForClient(client: MongoClient)

  def props(host: String, serviceManager: ActorRef) = Props(classOf[MongoDBConnector], host, serviceManager)

  def testMongoDBConnection(client: MongoClient) = Try{
    Await.result(client.listDatabaseNames().toFuture(), 2.seconds)
  }

  object MongoDBSink {
    def apply(parallelism: Int, collection: MongoCollection[Document])(implicit ex: ExecutionContext)
    : Sink[Document, Future[Done]] =
      Flow[Document]
        .mapAsyncUnordered(parallelism)(doc => collection.insertOne(doc).toFuture())
        .toMat(Sink.ignore)(Keep.right)
  }

}

class MongoDBConnector(host: String, serviceManager: ActorRef) extends Actor with ActorLogging{

  import MongoDBConnector._

  def receive: Receive = {
    case CheckConnectionForClient(client) =>
      log.info(s"Testing connection to MongoDB server at $host ...")
      testMongoDBConnection(client) match {
        case Success(databases) =>
          log.info(s"Connection to mongodb://$host established successfully, found databases: ${databases.mkString(",")}")
        case Failure(e) =>
          serviceManager ! ServiceManager.StopService
          log.error(s"Connection to mongodb://$host failed: $e")
      }
  }

}
