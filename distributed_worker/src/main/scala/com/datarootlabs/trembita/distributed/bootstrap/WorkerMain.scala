package com.datarootlabs.trembita.distributed.bootstrap


import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.{Config, ConfigFactory}
import com.datarootlabs.trembita.distributed.internal._


object WorkerMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val token: String = args match {
      case Array(t) => t
      case _        =>
        val config: Config = ConfigFactory.load()
        config.getString("lazy-collections.master.token")
    }
    val system = ActorSystem("computation-cluster")
    val workersCoordinatorActor = system.actorOf(Props(new WorkersCoordinatorActor(token)), name = "distributed")
    logger.info("Worker coordinator started")
  }
}
