package servicetools

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object SourceBroker {

  case object WorkAvailable
  case class TaskCompleted(src: DataSource)
  case class TaskFailed(src: DataSource, e: Throwable)

  def props(sourceEngines: Map[DataSource, ActorRef], pullSystem: ActorRef, failedTasks: ActorRef): Props =
    Props(classOf[SourceBroker], sourceEngines, pullSystem, failedTasks)

}

class SourceBroker(sourceEngines: Map[DataSource, ActorRef],
                   pullSystem: ActorRef,
                   failedTasks: ActorRef) extends Actor with ActorLogging {

  import SourceBroker._

  def receiveAvailable: Receive = {
    case src @ YoutubeSource(videoId) =>
      log.info(s"Starting task for $src ...")
      context.become(receiveBusy)
      sourceEngines.getOrElse(YoutubeSource, failedTasks) ! src
    case WorkAvailable => pullSystem ! PullSystem.WorkRequest
  }

  def receiveBusy: Receive = {
    case WorkAvailable => ()
    case TaskCompleted(src) =>
      log.info(s"Task completed for $src")
      context.become(receiveAvailable)
      pullSystem ! PullSystem.WorkRequest
    case task @ TaskFailed(src,e) =>
      failedTasks ! task
      context.become(receiveAvailable)
      pullSystem ! src
      pullSystem ! PullSystem.WorkRequest
  }

  def receive: Receive = receiveAvailable

}
