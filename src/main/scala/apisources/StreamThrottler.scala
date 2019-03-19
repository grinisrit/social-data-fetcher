package apisources

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.collection.immutable
import scala.concurrent.duration._

object StreamThrottler {

  case object PassRequest
  case object RequestFailed
  case object PassStatus
  case object OpenState

  def props(delay: FiniteDuration): Props =
    Props(classOf[StreamThrottler], delay)

}

class StreamThrottler(delay: FiniteDuration) extends Actor with ActorLogging{

  import StreamThrottler._
  import context.dispatcher

  var buffer: immutable.Queue[ActorRef] = immutable.Queue.empty

  def openState: Receive = {
    case PassRequest => sender() ! PassStatus
    case RequestFailed =>
      log.warning(s"Request failed, streams put on hold for $delay")
      context.become(closedState)
      context.system.scheduler.scheduleOnce(delay){ self ! OpenState }
  }

  def closedState: Receive = {
    case PassRequest =>
      log.warning(s"Streams on hold, buffer size ${buffer.size}")
      buffer = buffer.enqueue(sender())
    case OpenState =>
      log.warning("Releasing requests from buffer")
      context.become(openState)
      buffer.foreach(_ ! PassStatus)
      buffer = immutable.Queue.empty
  }

  def receive: Receive = openState

}
