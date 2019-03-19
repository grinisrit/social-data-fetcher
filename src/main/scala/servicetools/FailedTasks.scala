package servicetools

import akka.actor.{Actor, ActorLogging, Props}

object FailedTasks {
  def props: Props = Props(classOf[FailedTasks])
}

class FailedTasks extends Actor with ActorLogging {

  def receive: Receive = {
    case UnknownDataSource(str) =>
      log.error(s"Ignoring $str, source type currently not handled")
    case SourceBroker.TaskFailed(src,e) =>
      log.error(s"Task failed for $src: $e")
    case src: DataSource =>
      log.error(s"Service unavailable for $src")

  }

}
