package com.github.vitaliihonta.trembita.distributed.internal

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import MasterActor._
import WorkersCoordinatorActor._


class MasterActor(private val token: String) extends Actor with ActorLogging {
  private val cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = handle(Vector.empty)
  def handle(workers: Vector[ActorRef]): Receive = {
    case MemberUp(member)                      =>
      log.info("Member is Up: {}", member.address)
      register(member)
    case UnreachableMember(member)             =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info(
        "Member is Removed: {} after {}",
        member.address, previousStatus)

    case RegisterMe(`token`) =>
      log.info("Worker registered")
      val newWorker = sender()
      sender ! Registered
      context.become(handle(workers :+ newWorker))

    case RegisterMe(otherToken) =>
      log.info(s"Unknown token [$otherToken] received from ${sender().path}")

    case GetWorkers(count) =>
      val receiver = sender()
      workers.size match {
        case 0                     => receiver ! NoWorkersAvailable
        case availableWorkersCount =>
          val (selectedWorkers, balancingFactor) = count / availableWorkersCount match {
            case 0      => util.Random.shuffle(workers).take(count) -> 1
            case factor => util.Random.shuffle(workers) -> factor
          }
          val processorsCollector: ActorRef = context.actorOf(Props(new ProcessorsCollectorActor(receiver, count)))
          selectedWorkers.foreach(_ ! AllocateProcessors(processorsCollector, balancingFactor))
      }
  }

  private def register(member: Member): Unit =
    context.actorSelection(RootActorPath(member.address) / "*" / "distributed") ! RegisterWorker
}

object MasterActor {
  sealed trait Message extends Serializable
  case object RegisterWorker extends Message
  case class RegisterMe(token: String) extends Message
  case object Registered extends Message

  case class GetWorkers(count: Int) extends Message
  case class AvailableWorkers(workers: Vector[ActorRef]) extends Message
  case object NoWorkersAvailable extends Message
}

class ProcessorsCollectorActor(receiver: ActorRef, expectedProcessorsCount: Int) extends Actor with ActorLogging {
  def handle(processors: Vector[ActorRef], counter: Int): Receive = {
    case ProcessorsReady(newProcessors) =>
      log.info(s"Got new processors ${newProcessors.map(_.path).mkString("[", ", ", "]")}")
      val updatedProcessors = processors ++ newProcessors
      val updatedCounter = counter + newProcessors.size
      if (updatedCounter == expectedProcessorsCount) {
        log.info("Collected processors")
        receiver ! AvailableWorkers(updatedProcessors)
        context.stop(self)
      } else {
        log.info(s"There is some processors to collect [have: $updatedCounter, need: $expectedProcessorsCount]")
        context.become(handle(updatedProcessors, updatedCounter))
      }
  }

  override def receive: Receive = handle(Vector.empty, 0)
}
