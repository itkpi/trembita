//package com.datarootlabs.trembita.distributed
//
//
//import akka.actor._
//import akka.util.Timeout
//import internal._
//import scala.concurrent.{Await, ExecutionContext, Future}
//import scala.concurrent.duration._
//import com.datarootlabs.trembita._
//import com.datarootlabs.trembita.parallel._
//import com.datarootlabs.trembita.internal._
//
//
//trait DistributedDataPipeline[A] extends BaseDataPipeline[A] with Serializable {
//  def createActor: () => ActorRef
//
//  //todo: improve
//  override def mapAsync[B](timeout: FiniteDuration,
//                           parallelism: Int)
//                          (f: A => Future[B])(implicit ec: ExecutionContext): DistributedDataPipeline[B] =
//    new DistributedMapDataPipeline[A, B](createActor)({ a =>
//      val result: Future[B] = f(a)
//      Await.result(result, timeout)
//    })
//
//  //todo: improve
//  override def iterator: Iterator[A] = this.eval.iterator
//  override def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource[A](this.eval)
//  override def map[B](f: A => B): DistributedDataPipeline[B]
//  override def flatMap[B](f: A => Iterable[B]): DistributedDataPipeline[B]
//  override def filter(p: A => Boolean): DistributedDataPipeline[A]
//  override def collect[B](pf: PartialFunction[A, B]): DistributedDataPipeline[B]
//}
//
//object DistributedDataPipeline {
//  class ClusterConfiguration protected[distributed](val system: ActorSystem, val token: String)
//  class ComputationCluster(protected[distributed] val master: ActorRef,
//                           protected[distributed] val system: ActorSystem)
//
//  implicit val defaultTimeout: Timeout = Timeout(30.minutes)
//
//  def createConfig(token: String): ClusterConfiguration = {
//    val system = ActorSystem("computation-cluster")
//    new ClusterConfiguration(system, token)
//  }
//
//  def defaultConfig: ClusterConfiguration = {
//    import com.typesafe.config.{Config, ConfigFactory}
//
//    val config: Config = ConfigFactory.load()
//    val token: String = config.getString("lazy-collections.master.token")
//    createConfig(token)
//  }
//
//  def launchCluster(config: ClusterConfiguration): ComputationCluster = {
//    val master: ActorRef = config.system.actorOf(Props(new MasterActor(config.token)))
//    new ComputationCluster(master, config.system)
//  }
//
//  def apply[A](parallelism: Int)(source: () => Iterator[A])
//              (implicit cluster: ComputationCluster,
//               ec: ExecutionContext): DistributedDataPipeline[A] =
//    new DistributedSource[A](cluster.system, cluster.master, source, parallelism)
//}
