package servicetools

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.collection.mutable

object PullSystem {

  case object WorkRequest

  def props(sourceEngines: Map[DataSource, ActorRef], nBrokers: Int) =
    Props(classOf[PullSystem], sourceEngines, nBrokers)

}

class PullSystem(sourceEngines: Map[DataSource, ActorRef], nBrokers: Int) extends Actor with ActorLogging {

  import PullSystem._

  val sourcesPool: mutable.Queue[DataSource] = mutable.Queue.empty

  val failedTasks: ActorRef = context.actorOf(FailedTasks.props, "failed-tasks")
  val sourceBrokers: Seq[ActorRef] =
    (0 until nBrokers).map(i => context.actorOf(SourceBroker.props(sourceEngines, self, failedTasks), s"broker-$i"))

  def receivePoolEmpty: Receive = {
    case src: UnknownDataSource =>
      failedTasks ! src
    case src: DataSource =>
      sourcesPool.enqueue(src)
      context.become(receivePoolNonEmpty)
      sourceBrokers.foreach(_ ! SourceBroker.WorkAvailable)
    case WorkRequest => ()
  }

  def receivePoolNonEmpty: Receive = {
    case src: UnknownDataSource =>
      failedTasks ! src
    case src: DataSource =>
      sourcesPool.enqueue(src)
    case WorkRequest =>
      val source = sourcesPool.dequeue()
      sender() ! source
      if(sourcesPool.isEmpty){
        log.info("Pool is empty")
        context.become(receivePoolEmpty)
      }
  }

  def receive: Receive = receivePoolEmpty

}
