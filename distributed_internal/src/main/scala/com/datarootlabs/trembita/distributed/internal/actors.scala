package com.datarootlabs.trembita.distributed.internal

import akka.actor._
import scala.collection.mutable


object Communication {
  sealed trait Message extends Serializable
  case class TransformSource[A, B](transformation: Iterable[A] => Iterable[B]) extends Message
  case class Failed(cause: Exception) extends Message
  case class TransformationResult[B](result: Iterable[B]) extends Message
}

class CoordinatorActor[A](master: ActorRef,
                          parallelism: Int,
                          source: () => Iterator[A]) extends Actor with ActorLogging {

  import Communication._, MasterActor._, ProcessorActor._

  override def preStart(): Unit = master ! GetWorkers(parallelism)

  override def receive: Receive = {
    case NoWorkersAvailable =>
      log.warning("No workers are available")
      context.become(failed)

    case AvailableWorkers(workers) =>
      log.info(s"Got ${workers.size} workers")
      context.become(coordinate(workers))

    case TransformSource(transformation) =>
      log.info("Received transformation request before master response, waiting...")
      context.become(waitingForWorkers(sender(), transformation.asInstanceOf[Iterable[Any] => Iterable[Any]]))
  }

  def waitingForWorkers(receiver: ActorRef, transformation: Iterable[Any] => Iterable[Any]): Receive = {
    case NoWorkersAvailable =>
      log.warning("No workers are available")
      sendFailure(receiver, new Exception("No workers available"))
      self ! PoisonPill

    case AvailableWorkers(workers) =>
      log.info(s"[WAITING STATE] Got ${workers.size} workers")
      distributeWork(sender(), workers)(transformation)
  }

  def failed: Receive = {
    case TransformSource(_) =>
      sendFailure(sender(), new Exception("No workers available"))
      self ! PoisonPill
  }

  def coordinate(workers: Vector[ActorRef]): Receive = {
    case TransformSource(transformation) =>
      log.info("Received transformation request, working...")
      distributeWork(sender(), workers)(transformation.asInstanceOf[Iterable[Any] => Iterable[Any]])
  }

  def waitResult(receiver: ActorRef,
                 resultBuilder: mutable.Builder[Any, Vector[Any]],
                 progress: Map[ActorRef, Boolean]): Receive = {
    case TransformedItems(result) =>
      log.info("Got transformed items, collecting")
      resultBuilder ++= result
      val updatedProgress = progress.updated(sender(), true)
      if (isCompleted(updatedProgress)) {
        log.info("All tasks done")
        val result: Iterable[Any] = resultBuilder.result()
        receiver ! TransformationResult(result)
        self ! PoisonPill
      } else {
        log.info("Still have tasks to wait for")
        context.become(waitResult(receiver, resultBuilder, updatedProgress))
      }
  }

  private def isCompleted(progress: Map[ActorRef, Boolean]): Boolean = progress.exists(!_._2)

  private def sendFailure(receiver: ActorRef, cause: Exception): Unit =
    receiver ! Failed(new Exception("No workers available"))

  private def distributeWork(receiver: ActorRef, workers: Vector[ActorRef])
                            (transformation: Iterable[Any] => Iterable[Any]): Unit = {
    val iterator = source()
    var counter: Int = 0
    for (item <- iterator) {
      counter = if (counter < parallelism) counter else 0
      val worker: ActorRef = workers(counter)
      worker ! NewItem(item)
      counter += 1
    }
    workers.foreach(_ ! TransformItems(transformation))
    context.become(waitResult(receiver, Vector.newBuilder[Any], workers.map(_ -> false).toMap))
  }


  // todo: think about cleaning up workers
  override def postStop(): Unit = {
  }
}

class ProcessorActor extends Actor with ActorLogging {

  import ProcessorActor._

  def collect(itemsBuilder: mutable.Builder[Any, Vector[Any]]): Receive = {
    case NewItem(item) =>
      log.info(s"Got item $item")
      itemsBuilder += item
      context.become(collect(itemsBuilder))

    case TransformItems(func) =>
      log.info(s"Got transform request, working...")
      val coordinator: ActorRef = sender()
      val batch: Iterable[Any] = itemsBuilder.result()
      val result: Iterable[Any] = func(batch)
      coordinator ! TransformedItems(result)
      self ! PoisonPill
  }

  override def receive: Receive = collect(Vector.newBuilder[Any])
}

object ProcessorActor {
  sealed trait Message extends Serializable
  case class NewItem(item: Any) extends Message
  case class TransformItems(f: Iterable[Any] => Iterable[Any]) extends Message
  case class TransformedItems(result: Iterable[Any]) extends Message
}
