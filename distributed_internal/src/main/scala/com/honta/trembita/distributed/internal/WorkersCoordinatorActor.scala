package com.honta.trembita.distributed.internal


import akka.actor._
import MasterActor._
import WorkersCoordinatorActor._


class WorkersCoordinatorActor(masterToken: String) extends Actor with ActorLogging {
  def receive: Receive = {
    case RegisterWorker =>
      log.info("Received registration request")
      sender() ! RegisterMe(masterToken)

    case Registered =>
      log.info("Successfully registered myself")
      context.become(handle(sender()))
  }

  def handle(master: ActorRef): Receive = {
    case AllocateProcessors(collector, count) =>
      log.info(s"Allocating $count processors...")
      val processors = (1 to count).map(_ => context.actorOf(Props(new ProcessorActor)))
      collector ! ProcessorsReady(processors)
  }
}

object WorkersCoordinatorActor {
  sealed trait Message extends Serializable
  case class NewItem(item: Any) extends Message
  case class AllocateProcessors(collector: ActorRef, count: Int) extends Message
  case class ProcessorsReady(processors: Seq[ActorRef]) extends Message
}

