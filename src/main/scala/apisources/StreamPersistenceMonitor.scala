package apisources

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.Flow
import org.mongodb.scala.bson.collection.immutable.Document


object StreamPersistenceMonitor {

  case object DocumentToPersist

  def props(logEvery: Int, logOut: Int => Unit): Props =
    Props(classOf[StreamPersistenceMonitor], logEvery, logOut)

  def monitor(logEvery: Int)(logMessage: Int => Unit)(implicit system: ActorSystem): Flow[Document, Document, NotUsed] = {
    val monitorActor = system.actorOf(props(logEvery, logMessage))
    Flow[Document].map { element =>
      monitorActor ! DocumentToPersist
      element
    }
  }
}

class StreamPersistenceMonitor(logEvery: Int, logOut: Int => Unit) extends Actor{

  import StreamPersistenceMonitor._

  var nElements: Int = 0

  def receive: Receive = {
    case DocumentToPersist =>
      nElements += 1
      if(nElements % (logEvery+1) == 0) logOut(nElements-1)
  }

}
