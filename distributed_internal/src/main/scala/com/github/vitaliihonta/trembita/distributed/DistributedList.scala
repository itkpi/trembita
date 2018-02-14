package com.github.vitaliihonta.trembita.distributed


import akka.actor._
import akka.util.Timeout
import com.github.vitaliihonta.trembita._
import com.github.vitaliihonta.trembita.parallel._
import internal._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


trait DistributedList[A] extends BaseLazyList[A] with ParLazyList[A] with Serializable {
  def createActor: () => ActorRef

  //todo: improve
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int)
                          (f: A => Future[B])(implicit ec: ExecutionContext): DistributedList[B] =
    new DistributedMapList[A, B](createActor)({ a =>
      val result: Future[B] = f(a)
      Await.result(result, timeout)
    })

  //todo: improve
  override def iterator: Iterator[A] = this.force.iterator

  override def map[B](f: A => B): DistributedList[B]
  override def flatMap[B](f: A => Iterable[B]): DistributedList[B]
  override def filter(p: A => Boolean): DistributedList[A]
  override def collect[B](pf: PartialFunction[A, B]): DistributedList[B]
}

object DistributedList {
  class ClusterConfiguration protected[distributed](val system: ActorSystem, val token: String)
  class ComputationCluster(protected[distributed] val master: ActorRef,
                           protected[distributed] val system: ActorSystem)

  implicit val defaultTimeout: Timeout = Timeout(30.minutes)

  def createConfig(token: String): ClusterConfiguration = {
    val system = ActorSystem("computation-cluster")
    new ClusterConfiguration(system, token)
  }

  def defaultConfig: ClusterConfiguration = {
    import com.typesafe.config.{Config, ConfigFactory}

    val config: Config = ConfigFactory.load()
    val token: String = config.getString("lazy-collections.master.token")
    createConfig(token)
  }

  def launchCluster(config: ClusterConfiguration): ComputationCluster = {
    val master: ActorRef = config.system.actorOf(Props(new MasterActor(config.token)))
    new ComputationCluster(master, config.system)
  }

  def apply[A](parallelism: Int)(source: () => Iterator[A])
              (implicit cluster: ComputationCluster,
               ec: ExecutionContext): DistributedList[A] =
    new DistributedSource[A](cluster.system, cluster.master, source, parallelism)
}
